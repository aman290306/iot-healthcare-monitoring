#include <OneWire.h>
#include <DallasTemperature.h>
#include <string.h>
#include <stdlib.h>
#include <strings.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include "DHTesp.h"
#include <WiFi.h>
#include <FS.h>
#include <SPIFFS.h>
#include "Adafruit_MQTT.h"
#include "Adafruit_MQTT_Client.h"
#include "esp_task_wdt.h"
#include "esp_idf_version.h"
#include <ESP32Servo.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

#define TEMP_SENSOR 14
#define MOTION_DETECTOR 12
#define ENV_DHT_PIN 23   // DHT22 data pin -- room temp + humidity
#define BED_SERVO_PIN 27 // bed elevation servo -- matches servo1:PWM in diagram.json (was 13, unwired)

#define BUZZER_PIN_1 4
#define BUZZER_PIN_2 16
#define BUZZER_PIN_3 17
#define BUZZER_PIN_4 5
#define BUZZER_PIN_5 18   // BP alert
#define BUZZER_PIN_6 19   // ECG alert
#define BUZZER_PIN_7 15   // dosage critical-level buzzer
#define NETWORK_ALERT_PIN 26 // shared offline-status LED + buzzer
#define BUZZER_PIN_8 26
// Medication module outputs. Change MED_LED_PIN if your dosage LED is
// wired to a different GPIO; the dosage buzzer uses buzzer 7 above.
#define MED_LED_PIN 2
#define MED_BUZZER_PIN BUZZER_PIN_7

#define BED_ANGLE_MIN         0
#define BED_ANGLE_MAX         90
#define BED_ANGLE_SLEEPING    10
#define BED_ANGLE_BREATHING   45
#define BED_ANGLE_EMERGENCY   90
#define BED_STEP_DELAY_MS     60   // ms between each 1-degree step -- controls transition smoothness

// ---- Medication dosage (mg/hr) safety levels + ramp control ----
#define DOSAGE_MIN                 0
#define DOSAGE_MAX                 100
#define DOSAGE_WARNING_THRESHOLD   50   // >= this: warning level
#define DOSAGE_CRITICAL_THRESHOLD  80   // > this: critical level -- LED + buzzer alert
#define DOSAGE_TASK_TICK_MS        50   // medicationTask loop period
#define DOSAGE_RAMP_MG_PER_SEC     5.0  // max rate of change -- rate-limits abrupt slider jumps
#define DOSAGE_BLINK_INTERVAL_MS   150  // red LED / buzzer toggle period while critical

// Serial summary window: one minute.
#define AGGREGATION_WINDOW_MS 60000UL

// ---- Automatic adaptive monitoring interval (seconds) ----
#define SAMPLING_MIN_SECONDS       5
#define SAMPLING_MAX_SECONDS      60
#define SAMPLING_DEFAULT_SECONDS  30

// ---- Persistent offline logging + reconnect policy ----
#define OFFLINE_LOG_PATH "/offline.log"
#define OFFLINE_POS_PATH "/offline.pos"
#define RECONNECT_BACKOFF_MIN_MS 1000UL
#define RECONNECT_BACKOFF_MAX_MS 60000UL
#define OFFLINE_REPLAY_INTERVAL_MS 2000UL
#define HEALTH_ALERT_REPEAT_MS 30000UL
#define SYSTEM_STALE_MS 130000UL

// ---- Automatic condition-based bed angles (midpoints of the
// clinical ranges below). bedControlTask picks one of these whenever
// a relevant alert is active, overriding the manual slider/mode
// target -- see computeAutoBedAngle().
//
// Condition                | Range   | Reason
// Normal                   | 10-15   | Comfortable resting position
// Low SpO2                 | 45-60   | Upright position aids lung expansion/breathing
// High Temperature         | 15-30   | Comfort; no major elevation needed
// Low HR (Bradycardia)     | 10-20   | Avoid sudden elevation reducing cerebral blood flow
// High HR (Tachycardia)    | 30-45   | Reduces cardiac workload, improves comfort
// High BP                  | 30-45   | Semi-Fowler position reduces cardiovascular strain
// Low BP                   | 0-10    | Near-flat position maintains cerebral perfusion
// Multiple Critical Alerts | 60-90   | Emergency monitoring/access position
#define BED_ANGLE_AUTO_LOW_SPO2   52
#define BED_ANGLE_AUTO_HIGH_TEMP  22
#define BED_ANGLE_AUTO_LOW_HR     15
#define BED_ANGLE_AUTO_HIGH_HR    37
#define BED_ANGLE_AUTO_HIGH_BP    37
#define BED_ANGLE_AUTO_LOW_BP     5
#define BED_ANGLE_AUTO_MULTIPLE   75
#define BED_ANGLE_AUTO_NORMAL     12   // 10-15 deg: comfortable resting position, no alert active

#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
#define OLED_RESET -1
#define OLED_ADDRESS 0x3C    // OLED 1: vitals + status
#define OLED_ADDRESS2 0x3D   // OLED 2: dosage + motion + environment
#define WIFI_SSID "Wokwi-GUEST"
#define WIFI_PASS ""

#define IO_USERNAME "aman290306"
#define IO_KEY "aio_pkwv05iT6YWlnWugEPDf2ZGc5iVS"

#define AIO_SERVER "io.adafruit.com"
#define AIO_SERVERPORT 1883

// ---- Bed elevation control feeds ----
#define BED_ANGLE_FEED IO_USERNAME "/feeds/bed-angle-slider"   // subscribe: 0-90 manual angle slider
#define BED_ANGLE_STATUS_FEED IO_USERNAME "/feeds/bed-angle-status" // publish: actual current angle
#define BED_OVERRIDE_FEED IO_USERNAME "/feeds/bed-manual-override"  // subscribe: "on"/"off" -- staff toggle to disable auto-angle
#define BED_MODE_FEED IO_USERNAME "/feeds/bed-mode" // subscribe: auto/sleeping/breathing/emergency

// ---- Medication dosage control feeds ----
#define MED_DOSAGE_FEED IO_USERNAME "/feeds/dosage"  // subscribe: 0-100 mg/hr
#define FREQUENCY_FEED IO_USERNAME "/feeds/frequency" // subscribe: 5-60 seconds
#define VITALS_LOG_FEED IO_USERNAME "/feeds/vitals-log" // compact JSON readings


#define WDT_TIMEOUT_SEC 20

// ---- Predefined safe thresholds (used for alert feeds + as the
// reference values to punch into each Adafruit IO gauge widget) ----
#define SAFE_TEMP_MAX      38.0
#define SAFE_HR_MIN        60
#define SAFE_HR_MAX        120
#define SAFE_SPO2_MIN      85
#define SAFE_BP_SYS_MAX    140
#define SAFE_BP_SYS_MIN    90
#define SAFE_BP_DIA_MAX    90
#define SAFE_BP_DIA_MIN    60
#define SAFE_O2_MIN        19
#define SAFE_AQI_MAX       100

// Alert bit flags carried on alertQueue
#define ALERT_HIGH_TEMP  0x01
#define ALERT_LOW_HR     0x02
#define ALERT_HIGH_HR    0x04
#define ALERT_LOW_SPO2   0x08
#define ALERT_MOTION     0x10
#define ALERT_HIGH_BP    0x20
#define ALERT_LOW_BP     0x40
#define ALERT_ECG        0x80

#define PATIENT_ABNORMAL_FLAGS (ALERT_HIGH_TEMP | ALERT_LOW_HR | \
                                ALERT_HIGH_HR | ALERT_LOW_SPO2 | \
                                ALERT_HIGH_BP | ALERT_LOW_BP | ALERT_ECG)

WiFiClient client;

Adafruit_MQTT_Client mqtt(
  &client,
  AIO_SERVER,
  AIO_SERVERPORT,
  IO_USERNAME,
  IO_KEY
);

Adafruit_MQTT_Subscribe bedAngleSub(&mqtt, BED_ANGLE_FEED);
Adafruit_MQTT_Subscribe bedOverrideSub(&mqtt, BED_OVERRIDE_FEED);
Adafruit_MQTT_Subscribe bedModeSub(&mqtt, BED_MODE_FEED);
Adafruit_MQTT_Publish bedAngleStatusFeed(&mqtt, BED_ANGLE_STATUS_FEED);
Adafruit_MQTT_Publish vitalsLogFeed(&mqtt, VITALS_LOG_FEED);

Adafruit_MQTT_Subscribe medDosageSub(&mqtt, MED_DOSAGE_FEED);
Adafruit_MQTT_Subscribe frequencySub(&mqtt, FREQUENCY_FEED);

OneWire oneWire(TEMP_SENSOR);
DallasTemperature sensors(&oneWire);

DHTesp dhtSensor;

Servo bedServo;

// OLED 1: vitals + status. OLED 2: dosage + motion + environment. Both share
// the same I2C bus (Wire) but at different addresses (0x3C / 0x3D).
Adafruit_SSD1306 display(
  SCREEN_WIDTH,
  SCREEN_HEIGHT,
  &Wire,
  OLED_RESET
);

Adafruit_SSD1306 display2(
  SCREEN_WIDTH,
  SCREEN_HEIGHT,
  &Wire,
  OLED_RESET
);

typedef struct {
  int systolic;
  int diastolic;
} BPReading;

typedef struct {
  float roomTemp;   // real reading, DHT22
  float humidity;   // real reading, DHT22
  int aqi;          // simulated -- no AQI sensor available
  int o2Percent;    // simulated -- no O2 sensor available
} EnvReading;

// Aggregated snapshot produced by processingTask, consumed
// independently by displayTask and mqttTask.
typedef struct {
  float temperature;
  int heartRate;
  int spo2;
  int motionState;
  BPReading bp;
  int ecgAbnormal;
  EnvReading env;
  char statusMsg[64];
  uint8_t alertFlags;
  uint32_t sampleSequence;
} VitalsSnapshot;

enum SystemHealthState {
  HEALTH_ONLINE = 0,
  HEALTH_DEGRADED_MQTT = 1,
  HEALTH_OFFLINE_WIFI = 2,
  HEALTH_OFFLINE_SYSTEM = 3
};

QueueHandle_t hrQueue;
QueueHandle_t spo2Queue;
QueueHandle_t motionQueue;
QueueHandle_t alertQueue;
QueueHandle_t tempQueue;
QueueHandle_t bpQueue;
QueueHandle_t ecgQueue;
QueueHandle_t envQueue;
QueueHandle_t snapshotQueue;
QueueHandle_t mqttHealthQueue;
QueueHandle_t systemHealthQueue;
QueueHandle_t frequencyCommandQueue; // int seconds: slider value for stable conditions
QueueHandle_t samplingIntervalQueue; // int seconds: current automatic interval
QueueHandle_t bedCommandQueue;   // int: manual target angle from slider/mode buttons
QueueHandle_t bedStatusQueue;    // int: actual current servo angle, for mqttTask to publish
QueueHandle_t bedOverrideQueue;  // int: 0 = auto-angle enabled (default), 1 = staff has disabled it

QueueHandle_t medDosageCommandQueue;  // int: target dosage (mg/hr) from slider
QueueHandle_t medDosageStatusQueue;   // float: current ramped dosage, for OLED
QueueHandle_t medDosageLevelQueue;    // int: 0 normal / 1 warning / 2 critical

SemaphoreHandle_t oledMutex;
SemaphoreHandle_t alertSemaphore;
bool spiffsReady = false;

// Forward declaration -- computeAutoBedAngle() is defined further
// down but processingTask() (below) calls it before that point.
// Arduino IDE auto-generates prototypes for this, but not every
// toolchain (PlatformIO, arduino-cli, some CI/simulator builds)
// does, so it's declared explicitly here to be safe everywhere.
int computeAutoBedAngle(uint8_t alertFlags);
void handleIncomingSubscription(Adafruit_MQTT_Subscribe *subscription);
void updateSamplingInterval(uint8_t alertFlags);
void waitForNextSample();
String snapshotToJson(const VitalsSnapshot &snap);
bool appendOfflineSnapshot(const VitalsSnapshot &snap);
bool offlineRecordsPending();
bool readNextOfflineRecord(String &payload, size_t &nextOffset);
void commitOfflineRecord(size_t nextOffset);

// Select five seconds during abnormal conditions; otherwise use the
// doctor-selected frequency slider value.
void updateSamplingInterval(uint8_t alertFlags) {
  int requestedSeconds = SAMPLING_DEFAULT_SECONDS;
  if (frequencyCommandQueue != NULL) {
    xQueuePeek(frequencyCommandQueue, &requestedSeconds, 0);
  }
  requestedSeconds = constrain(requestedSeconds,
                               SAMPLING_MIN_SECONDS,
                               SAMPLING_MAX_SECONDS);

  bool abnormal = (alertFlags & PATIENT_ABNORMAL_FLAGS) != 0;
  int effectiveSeconds = abnormal ? SAMPLING_MIN_SECONDS
                                  : requestedSeconds;
  xQueueOverwrite(samplingIntervalQueue, &effectiveSeconds);
}

// Queue-aware non-blocking wait. Rechecking every 100 ms means a new
// five-second safety override can shorten a previously longer wait.
void waitForNextSample() {
  uint32_t startedAt = millis();

  while (1) {
    int intervalSeconds = SAMPLING_MIN_SECONDS;
    if (samplingIntervalQueue != NULL) {
      xQueuePeek(samplingIntervalQueue, &intervalSeconds, 0);
    }
    intervalSeconds = constrain(intervalSeconds,
                                SAMPLING_MIN_SECONDS,
                                SAMPLING_MAX_SECONDS);

    if (millis() - startedAt >= (uint32_t)intervalSeconds * 1000UL) {
      return;
    }

    esp_task_wdt_reset();
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

String snapshotToJson(const VitalsSnapshot &snap) {
  char payload[192];
  snprintf(payload, sizeof(payload),
           "{\"seq\":%lu,\"temp\":%.1f,\"hr\":%d,\"spo2\":%d,"
           "\"sys\":%d,\"dia\":%d,\"ecg\":%d,\"alerts\":%u}",
           (unsigned long)snap.sampleSequence,
           snap.temperature,
           snap.heartRate,
           snap.spo2,
           snap.bp.systolic,
           snap.bp.diastolic,
           snap.ecgAbnormal,
           snap.alertFlags);
  return String(payload);
}

size_t readOfflineOffset() {
  File positionFile = SPIFFS.open(OFFLINE_POS_PATH, FILE_READ);
  if (!positionFile) return 0;
  size_t offset = (size_t)strtoul(positionFile.readString().c_str(), NULL, 10);
  positionFile.close();
  return offset;
}

bool appendOfflineSnapshot(const VitalsSnapshot &snap) {
  if (!spiffsReady) return false;
  File logFile = SPIFFS.open(OFFLINE_LOG_PATH, FILE_APPEND);
  if (!logFile) {
    Serial.println("SPIFFS ERROR: could not open offline log");
    return false;
  }

  String payload = snapshotToJson(snap);
  bool stored = logFile.println(payload) > 0;
  logFile.close();

  if (stored) {
    Serial.print("SPIFFS stored snapshot ");
    Serial.println(snap.sampleSequence);
  } else {
    Serial.println("SPIFFS ERROR: snapshot write failed");
  }
  return stored;
}

bool offlineRecordsPending() {
  if (!spiffsReady || !SPIFFS.exists(OFFLINE_LOG_PATH)) return false;
  File logFile = SPIFFS.open(OFFLINE_LOG_PATH, FILE_READ);
  if (!logFile) return false;
  bool pending = readOfflineOffset() < logFile.size();
  logFile.close();
  return pending;
}

bool readNextOfflineRecord(String &payload, size_t &nextOffset) {
  if (!spiffsReady) return false;
  File logFile = SPIFFS.open(OFFLINE_LOG_PATH, FILE_READ);
  if (!logFile) return false;

  size_t offset = readOfflineOffset();
  if (offset >= logFile.size()) {
    logFile.close();
    SPIFFS.remove(OFFLINE_LOG_PATH);
    SPIFFS.remove(OFFLINE_POS_PATH);
    return false;
  }

  if (!logFile.seek(offset)) {
    logFile.close();
    Serial.println("SPIFFS ERROR: invalid replay offset");
    return false;
  }

  payload = logFile.readStringUntil('\n');
  payload.trim();
  nextOffset = logFile.position();
  logFile.close();
  return payload.length() > 0;
}

void commitOfflineRecord(size_t nextOffset) {
  if (!spiffsReady) return;
  File logFile = SPIFFS.open(OFFLINE_LOG_PATH, FILE_READ);
  size_t logSize = logFile ? logFile.size() : 0;
  if (logFile) logFile.close();

  if (nextOffset >= logSize) {
    SPIFFS.remove(OFFLINE_LOG_PATH);
    SPIFFS.remove(OFFLINE_POS_PATH);
    return;
  }

  File positionFile = SPIFFS.open(OFFLINE_POS_PATH, FILE_WRITE);
  if (!positionFile) {
    Serial.println("SPIFFS ERROR: could not save replay position");
    return;
  }
  positionFile.print((unsigned long)nextOffset);
  positionFile.close();
}

// ---------------------------------------------------------------
// sensorTask: core patient vitals -- HR, SpO2, temperature, motion.
// BP, ECG, and environment each get their own dedicated task below.
// ---------------------------------------------------------------
void sensorTask(void *param) {

  esp_task_wdt_add(NULL);

  int heartRate;
  int spo2;
  float temperature = 0;
  int motionState;

  while (1) {

    heartRate = random(45, 150);
    spo2 = random(60, 100);

    sensors.requestTemperatures();
    temperature = sensors.getTempCByIndex(0);

    motionState = digitalRead(MOTION_DETECTOR);

    xQueueOverwrite(hrQueue, &heartRate);
    xQueueOverwrite(spo2Queue, &spo2);
    xQueueOverwrite(tempQueue, &temperature);
    xQueueOverwrite(motionQueue, &motionState);

    esp_task_wdt_reset();
    waitForNextSample();
  }
}

// ---------------------------------------------------------------
// bpTask: dedicated blood pressure task.
// ---------------------------------------------------------------
void bpTask(void *param) {

  esp_task_wdt_add(NULL);

  BPReading bp;

  while (1) {
    bp.systolic = random(85, 180);
    bp.diastolic = random(55, 110);

    xQueueOverwrite(bpQueue, &bp);

    esp_task_wdt_reset();
    waitForNextSample();
  }
}

// ---------------------------------------------------------------
// ecgTask: dedicated ECG task.
// ---------------------------------------------------------------
void ecgTask(void *param) {

  esp_task_wdt_add(NULL);

  int ecgAbnormal;

  while (1) {
    // ~30% chance of a simulated abnormal ECG reading (arrhythmia)
    ecgAbnormal = (random(0, 10) < 3) ? 1 : 0;

    xQueueOverwrite(ecgQueue, &ecgAbnormal);

    esp_task_wdt_reset();
    waitForNextSample();
  }
}

// ---------------------------------------------------------------
// environmentTask: dedicated environment-monitoring task. Real
// DHT22 readings for room temp/humidity; AQI is simulated since no
// AQI sensor is available.
// ---------------------------------------------------------------
void environmentTask(void *param) {

  esp_task_wdt_add(NULL);

  EnvReading env;
  env.roomTemp = 0;
  env.humidity = 0;

  while (1) {

    TempAndHumidity dhtData = dhtSensor.getTempAndHumidity();
    if (!isnan(dhtData.temperature)) env.roomTemp = dhtData.temperature;
    if (!isnan(dhtData.humidity)) env.humidity = dhtData.humidity;

    env.aqi = random(20, 180);
    env.o2Percent = random(18, 22);

    xQueueOverwrite(envQueue, &env);

    esp_task_wdt_reset();
    waitForNextSample();
  }
}

// ---------------------------------------------------------------
// alertTask: owns the buzzers. Wakes on alertSemaphore, reads the
// latest alert flags from alertQueue, and fires the matching buzzer.
// ---------------------------------------------------------------
void alertTask(void *param) {

  esp_task_wdt_add(NULL);

  uint8_t alertFlags;

  while (1) {

    if (xSemaphoreTake(alertSemaphore, pdMS_TO_TICKS(2000)) == pdTRUE) {

      if (xQueuePeek(alertQueue, &alertFlags, 0) == pdTRUE) {

        if (alertFlags & ALERT_HIGH_TEMP) {
          digitalWrite(BUZZER_PIN_1, HIGH);
          vTaskDelay(pdMS_TO_TICKS(500));
          digitalWrite(BUZZER_PIN_1, LOW);
        }

        if (alertFlags & (ALERT_LOW_HR | ALERT_HIGH_HR)) {
          digitalWrite(BUZZER_PIN_2, HIGH);
          vTaskDelay(pdMS_TO_TICKS(500));
          digitalWrite(BUZZER_PIN_2, LOW);
        }

        if (alertFlags & ALERT_LOW_SPO2) {
          digitalWrite(BUZZER_PIN_3, HIGH);
          vTaskDelay(pdMS_TO_TICKS(500));
          digitalWrite(BUZZER_PIN_3, LOW);
        }

        if (alertFlags & ALERT_MOTION) {
          digitalWrite(BUZZER_PIN_4, HIGH);
          vTaskDelay(pdMS_TO_TICKS(500));
          digitalWrite(BUZZER_PIN_4, LOW);
        }

        if (alertFlags & (ALERT_HIGH_BP | ALERT_LOW_BP)) {
          digitalWrite(BUZZER_PIN_5, HIGH);
          vTaskDelay(pdMS_TO_TICKS(500));
          digitalWrite(BUZZER_PIN_5, LOW);
        }

        if (alertFlags & ALERT_ECG) {
          digitalWrite(BUZZER_PIN_6, HIGH);
          vTaskDelay(pdMS_TO_TICKS(500));
          digitalWrite(BUZZER_PIN_6, LOW);
        }
      }
    }

    esp_task_wdt_reset();
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

// ---------------------------------------------------------------
// systemHealthTask: ONLINE when Wi-Fi/MQTT/sensing are healthy,
// DEGRADED when only MQTT is down, and OFFLINE for Wi-Fi or sensing
// failure. GPIO 26 drives the LED and buzzer together: MQTT=2,
// Wi-Fi=3, stale sensing=4 pulses.
// ---------------------------------------------------------------
void systemHealthTask(void *param) {

  esp_task_wdt_add(NULL);
  pinMode(NETWORK_ALERT_PIN, OUTPUT);
  digitalWrite(NETWORK_ALERT_PIN, LOW);

  int previousState = -1;
  int mqttConnected = 0;
  uint32_t lastAlertAt = 0;
  uint32_t lastSnapshotAt = millis();
  uint32_t lastSequence = 0;

  while (1) {
    uint32_t now = millis();
    xQueuePeek(mqttHealthQueue, &mqttConnected, 0);

    VitalsSnapshot snap;
    if (xQueuePeek(snapshotQueue, &snap, 0) == pdTRUE &&
        snap.sampleSequence != lastSequence) {
      lastSequence = snap.sampleSequence;
      lastSnapshotAt = now;
    }

    int state;
    if (WiFi.status() != WL_CONNECTED) state = HEALTH_OFFLINE_WIFI;
    else if (now - lastSnapshotAt > SYSTEM_STALE_MS) state = HEALTH_OFFLINE_SYSTEM;
    else if (!mqttConnected) state = HEALTH_DEGRADED_MQTT;
    else state = HEALTH_ONLINE;

    xQueueOverwrite(systemHealthQueue, &state);

    bool alertDue = state != HEALTH_ONLINE &&
                    (state != previousState ||
                     now - lastAlertAt >= HEALTH_ALERT_REPEAT_MS);
    if (alertDue) {
      int pulses = state == HEALTH_DEGRADED_MQTT ? 2 :
                   state == HEALTH_OFFLINE_WIFI ? 3 : 4;
      for (int i = 0; i < pulses; i++) {
        digitalWrite(NETWORK_ALERT_PIN, HIGH);
        vTaskDelay(pdMS_TO_TICKS(100));
        digitalWrite(NETWORK_ALERT_PIN, LOW);
        vTaskDelay(pdMS_TO_TICKS(120));
        esp_task_wdt_reset();
      }
      lastAlertAt = millis();
    }

    previousState = state;
    esp_task_wdt_reset();
    vTaskDelay(pdMS_TO_TICKS(500));
  }
}

// ---------------------------------------------------------------
// processingTask: decision/aggregation logic ONLY. Reads every
// sensor queue, evaluates thresholds, builds the combined status
// word and alert flags, and publishes ONE VitalsSnapshot for
// displayTask and mqttTask to each consume independently. Does NOT
// touch the display or the network -- that separation is the point.
// ---------------------------------------------------------------
void processingTask(void *param) {

  esp_task_wdt_add(NULL);

  VitalsSnapshot snap;
  uint32_t sampleSequence = 0;

  while (1) {

    // A 1000ms timeout (instead of 0) matters most on the very first
    // loop iteration after boot: sensorTask/bpTask/ecgTask/
    // environmentTask run at lower priority, so without a real
    // timeout here, processingTask (priority 2) could peek before
    // any of them ever write -- producing a garbage/zero first
    // reading (e.g. HR always starting at 0) instead of waiting for
    // real data.
    xQueuePeek(hrQueue, &snap.heartRate, pdMS_TO_TICKS(1000));
    xQueuePeek(spo2Queue, &snap.spo2, pdMS_TO_TICKS(1000));
    xQueuePeek(tempQueue, &snap.temperature, pdMS_TO_TICKS(1000));
    xQueuePeek(motionQueue, &snap.motionState, pdMS_TO_TICKS(1000));
    xQueuePeek(bpQueue, &snap.bp, pdMS_TO_TICKS(1000));
    xQueuePeek(ecgQueue, &snap.ecgAbnormal, pdMS_TO_TICKS(1000));
    xQueuePeek(envQueue, &snap.env, pdMS_TO_TICKS(1000));

    strcpy(snap.statusMsg, "OK");
    snap.alertFlags = 0;

    // Every condition below is independent -- none suppress each
    // other. Any combination can show together in the status word.

    if (snap.temperature > SAFE_TEMP_MAX) {
      snap.alertFlags |= ALERT_HIGH_TEMP;
      if (strcmp(snap.statusMsg, "OK") == 0) strcpy(snap.statusMsg, "High Temp");
      else strcat(snap.statusMsg, " + High Temp");
    }

    if (snap.heartRate < SAFE_HR_MIN && snap.heartRate != 0) {
      snap.alertFlags |= ALERT_LOW_HR;
      if (strcmp(snap.statusMsg, "OK") == 0) strcpy(snap.statusMsg, "Low HR");
      else strcat(snap.statusMsg, " + Low HR");
    }
    else if (snap.heartRate > SAFE_HR_MAX) {
      snap.alertFlags |= ALERT_HIGH_HR;
      if (strcmp(snap.statusMsg, "OK") == 0) strcpy(snap.statusMsg, "High HR");
      else strcat(snap.statusMsg, " + High HR");
    }

    if (snap.spo2 < SAFE_SPO2_MIN && snap.spo2 != 0) {
      snap.alertFlags |= ALERT_LOW_SPO2;
      if (strcmp(snap.statusMsg, "OK") == 0) strcpy(snap.statusMsg, "Low SpO2");
      else strcat(snap.statusMsg, " + Low SpO2");
    }

    if (snap.bp.systolic > SAFE_BP_SYS_MAX || snap.bp.diastolic > SAFE_BP_DIA_MAX) {
      snap.alertFlags |= ALERT_HIGH_BP;
      if (strcmp(snap.statusMsg, "OK") == 0) strcpy(snap.statusMsg, "High BP");
      else strcat(snap.statusMsg, " + High BP");
    }
    else if ((snap.bp.systolic < SAFE_BP_SYS_MIN && snap.bp.systolic != 0) ||
             (snap.bp.diastolic < SAFE_BP_DIA_MIN && snap.bp.diastolic != 0)) {
      snap.alertFlags |= ALERT_LOW_BP;
      if (strcmp(snap.statusMsg, "OK") == 0) strcpy(snap.statusMsg, "Low BP");
      else strcat(snap.statusMsg, " + Low BP");
    }

    if (snap.ecgAbnormal) {
      snap.alertFlags |= ALERT_ECG;
      // ECG intentionally left out of the status word (buzzer/LED only)
    }

    if (snap.motionState) {
      snap.alertFlags |= ALERT_MOTION;
      // Motion also left out of the status word -- shown on its own
      // dedicated line on OLED2 instead.
    }

    updateSamplingInterval(snap.alertFlags);

    snap.sampleSequence = ++sampleSequence;
    xQueueOverwrite(snapshotQueue, &snap);

    Serial.print("Status: ");
    Serial.print(snap.statusMsg);
    Serial.print(" | Bed angle needed: ");
    int neededAngle = computeAutoBedAngle(snap.alertFlags);
    Serial.println(neededAngle >= 0 ? neededAngle : BED_ANGLE_AUTO_NORMAL);

    int effectiveSamplingSeconds = SAMPLING_MIN_SECONDS;
    xQueuePeek(samplingIntervalQueue, &effectiveSamplingSeconds, 0);
    Serial.print("Sampling: ");
    Serial.print(effectiveSamplingSeconds);
    Serial.println((snap.alertFlags & PATIENT_ABNORMAL_FLAGS)
                     ? " sec (abnormal: automatic)"
                     : " sec (normal: frequency slider)");

    if (snap.alertFlags != 0) {
      xQueueOverwrite(alertQueue, &snap.alertFlags);
      xSemaphoreGive(alertSemaphore);
    }

    esp_task_wdt_reset();
    waitForNextSample();
  }
}

// ---------------------------------------------------------------
// dataAggregationTask: prints one-minute averages to Serial only.
// The number of samples naturally changes with the adaptive rate.
// ---------------------------------------------------------------
void dataAggregationTask(void *param) {

  esp_task_wdt_add(NULL);

  VitalsSnapshot snap;
  uint32_t lastSequence = 0;
  uint32_t samples = 0;
  float heartRateTotal = 0;
  float spo2Total = 0;
  float systolicTotal = 0;
  float diastolicTotal = 0;
  uint32_t windowStartedAt = millis();

  while (1) {
    if (xQueuePeek(snapshotQueue, &snap, pdMS_TO_TICKS(1000)) == pdTRUE &&
        snap.sampleSequence != lastSequence) {

      lastSequence = snap.sampleSequence;

      // Ignore an incomplete startup snapshot rather than pulling
      // the first one-minute average down with zero values.
      if (snap.heartRate > 0 && snap.spo2 > 0 &&
          snap.bp.systolic > 0 && snap.bp.diastolic > 0) {
        heartRateTotal += snap.heartRate;
        spo2Total += snap.spo2;
        systolicTotal += snap.bp.systolic;
        diastolicTotal += snap.bp.diastolic;
        samples++;
      }

    }

    if (millis() - windowStartedAt >= AGGREGATION_WINDOW_MS) {
      if (samples > 0) {
        Serial.print("1-min averages | HR: ");
        Serial.print(heartRateTotal / samples, 1);
        Serial.print(" bpm | SpO2: ");
        Serial.print(spo2Total / samples, 1);
        Serial.print("% | BP: ");
        Serial.print(systolicTotal / samples, 1);
        Serial.print("/");
        Serial.print(diastolicTotal / samples, 1);
        Serial.print(" | samples: ");
        Serial.println(samples);
      }

      samples = 0;
      heartRateTotal = 0;
      spo2Total = 0;
      systolicTotal = 0;
      diastolicTotal = 0;
      windowStartedAt = millis();
    }

    esp_task_wdt_reset();
    vTaskDelay(pdMS_TO_TICKS(500));
  }
}

// ---------------------------------------------------------------
// displayTask: owns BOTH OLEDs exclusively. Reads the latest
// snapshot and renders it. Runs independently of MQTT -- a slow or
// blocked network call can never freeze the display.
// ---------------------------------------------------------------
void displayTask(void *param) {

  esp_task_wdt_add(NULL);

  VitalsSnapshot snap;

  while (1) {

    if (xQueuePeek(snapshotQueue, &snap, pdMS_TO_TICKS(1000)) == pdTRUE) {

      if (xSemaphoreTake(oledMutex, pdMS_TO_TICKS(200)) == pdTRUE) {

        // ---- OLED 1 (0x3C): vitals + status ----
        display.clearDisplay();

        display.setCursor(0, 0);
        display.print("Temp:");
        display.print(snap.temperature);
        display.print("C");

        display.setCursor(0, 9);
        display.print("HR:");
        display.print(snap.heartRate);

        display.setCursor(0, 18);
        display.print("SpO2:");
        display.print(snap.spo2);
        display.print("%");

        display.setCursor(0, 27);
        display.print("BP:");
        display.print(snap.bp.systolic);
        display.print("/");
        display.print(snap.bp.diastolic);

        display.setCursor(0, 36);
        display.print("ECG:");
        display.print(snap.ecgAbnormal ? "Abnormal" : "Normal");

        display.setCursor(0, 46);
        display.print(snap.statusMsg);

        int healthState = HEALTH_OFFLINE_WIFI;
        xQueuePeek(systemHealthQueue, &healthState, 0);
        // Keep the full lower display area available for patient status
        // while online. Only connectivity failures claim the bottom row.
        if (healthState != HEALTH_ONLINE) {
          display.fillRect(0, 54, SCREEN_WIDTH, 10, SSD1306_BLACK);
          display.setCursor(0, 56);
          if (healthState == HEALTH_DEGRADED_MQTT) {
            display.print("DEGRADED: LOGGING");
          } else if (healthState == HEALTH_OFFLINE_SYSTEM) {
            display.print("SYSTEM OFFLINE");
          } else {
            display.print("LOGGING OFFLINE");
          }
        }

        // ---- Medication dosage (module 3) -- non-blocking peek,
        // medicationTask updates this queue independently of the
        // vitals snapshot cadence. ----
        float dosage = 0;
        int dosageLevel = 0;
        xQueuePeek(medDosageStatusQueue, &dosage, 0);
        xQueuePeek(medDosageLevelQueue, &dosageLevel, 0);

        display.display();

        // ---- OLED 2 (0x3D): dosage + motion + environment ----
        display2.clearDisplay();

        display2.setCursor(0, 0);
        display2.print("Dose:");
        display2.print((int)round(dosage));
        display2.print("mg/hr ");
        display2.print(dosageLevel == 2 ? "CRIT" : (dosageLevel == 1 ? "WARN" : "OK"));

        display2.setCursor(0, 11);
        display2.print("Motion:");
        display2.print(snap.motionState ? "Yes" : "No");

        display2.setCursor(0, 22);
        display2.print("Room:");
        display2.print(snap.env.roomTemp);
        display2.print("C");

        display2.setCursor(0, 33);
        display2.print("Humidity:");
        display2.print(snap.env.humidity);
        display2.print("%");

        display2.setCursor(0, 44);
        display2.print("O2:");
        display2.print(snap.env.o2Percent);
        display2.print("%");

        display2.setCursor(72, 44);
        display2.print("AQI:");
        display2.print(snap.env.aqi);

        display2.display();

        xSemaphoreGive(oledMutex);
      }
    }

    esp_task_wdt_reset();
    // Limit I2C redraw traffic while keeping dosage changes responsive.
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

// ---------------------------------------------------------------
// handleIncomingSubscription: dispatches one incoming MQTT message
// to the correct FreeRTOS command queue.
// ---------------------------------------------------------------
void handleIncomingSubscription(Adafruit_MQTT_Subscribe *subscription) {

  if (subscription == &bedAngleSub) {
    int sliderAngle = atoi((char *)bedAngleSub.lastread);
    sliderAngle = constrain(sliderAngle, BED_ANGLE_MIN, BED_ANGLE_MAX);
    xQueueOverwrite(bedCommandQueue, &sliderAngle);
    Serial.print("Bed slider -> ");
    Serial.println(sliderAngle);
  }
  else if (subscription == &bedModeSub) {
    char *mode = (char *)bedModeSub.lastread;
    int targetAngle = BED_ANGLE_AUTO_NORMAL;
    int manualOverride = 0;

    if (strcasecmp(mode, "auto") == 0) {
      // Restore automatic condition-based control and a safe resting
      // fallback for periods when no patient alert is active.
      targetAngle = BED_ANGLE_AUTO_NORMAL;
      manualOverride = 0;
    }
    else if (strcasecmp(mode, "sleeping") == 0) {
      targetAngle = BED_ANGLE_SLEEPING;
      manualOverride = 1;
    }
    else if (strcasecmp(mode, "breathing") == 0) {
      targetAngle = BED_ANGLE_BREATHING;
      manualOverride = 1;
    }
    else if (strcasecmp(mode, "emergency") == 0) {
      targetAngle = BED_ANGLE_EMERGENCY;
      manualOverride = 1;
    }
    else {
      Serial.print("Unknown bed mode: ");
      Serial.println(mode);
      return;
    }

    xQueueOverwrite(bedCommandQueue, &targetAngle);
    xQueueOverwrite(bedOverrideQueue, &manualOverride);
    Serial.print("Bed mode -> ");
    Serial.print(mode);
    Serial.print(" | target: ");
    Serial.print(targetAngle);
    Serial.println(" deg");
  }
  else if (subscription == &bedOverrideSub) {
    char *ovStr = (char *)bedOverrideSub.lastread;
    int overrideOn = (strcasecmp(ovStr, "on") == 0 || strcmp(ovStr, "1") == 0) ? 1 : 0;
    xQueueOverwrite(bedOverrideQueue, &overrideOn);
    Serial.print("Manual override -> ");
    Serial.println(overrideOn ? "ON (auto-angle disabled)" : "OFF (auto-angle enabled)");
  }
  else if (subscription == &medDosageSub) {
    int dosageTarget = atoi((char *)medDosageSub.lastread);
    dosageTarget = constrain(dosageTarget, DOSAGE_MIN, DOSAGE_MAX);
    xQueueOverwrite(medDosageCommandQueue, &dosageTarget);
    Serial.print("Dosage target -> ");
    Serial.println(dosageTarget);
  }
  else if (subscription == &frequencySub) {
    int requestedSeconds = atoi((char *)frequencySub.lastread);
    requestedSeconds = constrain(requestedSeconds,
                                 SAMPLING_MIN_SECONDS,
                                 SAMPLING_MAX_SECONDS);
    xQueueOverwrite(frequencyCommandQueue, &requestedSeconds);

    // Apply the new slider value immediately if conditions are normal.
    // Active abnormal conditions retain the automatic five-second rate.
    VitalsSnapshot latestSnap;
    uint8_t latestFlags = 0;
    if (xQueuePeek(snapshotQueue, &latestSnap, 0) == pdTRUE) {
      latestFlags = latestSnap.alertFlags;
    }
    updateSamplingInterval(latestFlags);

    Serial.print("Normal sampling interval -> ");
    Serial.print(requestedSeconds);
    Serial.println(" sec");
  }
}

// ---------------------------------------------------------------
// mqttTask: owns the MQTT connection and every publish. Runs
// independently of the display -- a slow OLED redraw or a slow
// network call never blocks the other.
// ---------------------------------------------------------------
void mqttTask(void *param) {

  esp_task_wdt_add(NULL);

  VitalsSnapshot snap;
  uint32_t lastHandledSequence = 0;
  uint32_t lastStatusPublish = 0;
  uint32_t lastPing = 0;
  uint32_t lastReplayAt = 0;
  uint32_t nextReconnectAt = 0;
  uint32_t reconnectBackoff = RECONNECT_BACKOFF_MIN_MS;
  bool wifiWasConnected = WiFi.status() == WL_CONNECTED;

  while (1) {
    uint32_t now = millis();

    // Send a live record directly only when no older SPIFFS records
    // are pending. Otherwise append it to preserve chronological order.
    if (xQueuePeek(snapshotQueue, &snap, 0) == pdTRUE &&
        snap.sampleSequence != lastHandledSequence) {
      lastHandledSequence = snap.sampleSequence;
      bool mustStore = !mqtt.connected() || offlineRecordsPending();

      if (!mustStore) {
        String payload = snapshotToJson(snap);
        if (!vitalsLogFeed.publish(payload.c_str())) {
          mustStore = true;
          mqtt.disconnect();
          nextReconnectAt = now + reconnectBackoff;
        }
      }

      if (mustStore && !appendOfflineSnapshot(snap)) {
        Serial.println("CRITICAL: snapshot could not be persisted");
      }
    }

    bool wifiConnected = WiFi.status() == WL_CONNECTED;

    if (!wifiConnected) {
      if (mqtt.connected()) mqtt.disconnect();

      if ((int32_t)(now - nextReconnectAt) >= 0) {
        Serial.println("WiFi offline: reconnect requested");
        WiFi.reconnect();
        nextReconnectAt = now + reconnectBackoff;
        reconnectBackoff = min(reconnectBackoff * 2,
                               (uint32_t)RECONNECT_BACKOFF_MAX_MS);
      }
    } else {
      if (!wifiWasConnected) {
        Serial.println("WiFi restored");
        nextReconnectAt = now;
        reconnectBackoff = RECONNECT_BACKOFF_MIN_MS;
      }

      if (!mqtt.connected() && (int32_t)(now - nextReconnectAt) >= 0) {
        mqtt.disconnect();
        int8_t connectResult = mqtt.connect();
        if (connectResult == 0) {
          Serial.println("MQTT Connected");
          reconnectBackoff = RECONNECT_BACKOFF_MIN_MS;
          lastPing = now;
        } else {
          Serial.print("MQTT reconnect failed, code=");
          Serial.println(connectResult);
          Serial.println(mqtt.connectErrorString(connectResult));
          nextReconnectAt = now + reconnectBackoff;
          reconnectBackoff = min(reconnectBackoff * 2,
                                 (uint32_t)RECONNECT_BACKOFF_MAX_MS);
        }
      }
    }

    wifiWasConnected = wifiConnected;

    if (mqtt.connected()) {
      if (now - lastPing >= 30000) {
        if (!mqtt.ping()) {
          Serial.println("MQTT ping failed");
          mqtt.disconnect();
          nextReconnectAt = now + reconnectBackoff;
        }
        lastPing = now;
      }

      if (mqtt.connected()) {
        Adafruit_MQTT_Subscribe *subscription;
        while ((subscription = mqtt.readSubscription(200))) {
          handleIncomingSubscription(subscription);
        }

        // At-least-once replay: advance the persistent read offset only
        // after the cloud publish succeeds.
        if (now - lastReplayAt >= OFFLINE_REPLAY_INTERVAL_MS) {
          String payload;
          size_t nextOffset = 0;
          if (readNextOfflineRecord(payload, nextOffset)) {
            if (vitalsLogFeed.publish(payload.c_str())) {
              commitOfflineRecord(nextOffset);
              Serial.println("SPIFFS record synchronized");
            } else {
              Serial.println("Replay failed; SPIFFS record retained");
              mqtt.disconnect();
              nextReconnectAt = now + reconnectBackoff;
            }
          }
          lastReplayAt = now;
        }

        if (now - lastStatusPublish >= 5000) {
          int currentBedAngle;
          if (xQueuePeek(bedStatusQueue, &currentBedAngle, 0) == pdTRUE) {
            bedAngleStatusFeed.publish((int32_t)currentBedAngle);
          }
          lastStatusPublish = now;
        }
      }
    }

    int mqttConnected = mqtt.connected() ? 1 : 0;
    xQueueOverwrite(mqttHealthQueue, &mqttConnected);

    esp_task_wdt_reset();
    // Poll controls frequently so dashboard sliders feel responsive.
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

// ---------------------------------------------------------------
// computeAutoBedAngle: maps the patient's current alert flags to a
// clinically recommended bed angle (see the condition -> angle
// table above BED_ANGLE_AUTO_* ). Returns -1 when no physiological
// condition here warrants an automatic angle, so bedControlTask
// falls back to the manual slider/mode target. ECG and motion
// alerts have no bed-angle recommendation and are ignored. When two
// or more distinct categories (SpO2 / temp / HR / BP) are active at
// once, this escalates to the "multiple critical alerts" range
// instead of picking one condition arbitrarily over another.
// ---------------------------------------------------------------
int computeAutoBedAngle(uint8_t alertFlags) {

  bool lowSpo2  = alertFlags & ALERT_LOW_SPO2;
  bool highTemp = alertFlags & ALERT_HIGH_TEMP;
  bool hrIssue  = alertFlags & (ALERT_LOW_HR | ALERT_HIGH_HR);
  bool bpIssue  = alertFlags & (ALERT_HIGH_BP | ALERT_LOW_BP);

  int activeCategories = lowSpo2 + highTemp + hrIssue + bpIssue;

  if (activeCategories >= 2) {
    return BED_ANGLE_AUTO_MULTIPLE;
  }

  if (lowSpo2)  return BED_ANGLE_AUTO_LOW_SPO2;
  if (highTemp) return BED_ANGLE_AUTO_HIGH_TEMP;
  if (alertFlags & ALERT_LOW_HR)  return BED_ANGLE_AUTO_LOW_HR;
  if (alertFlags & ALERT_HIGH_HR) return BED_ANGLE_AUTO_HIGH_HR;
  if (alertFlags & ALERT_HIGH_BP) return BED_ANGLE_AUTO_HIGH_BP;
  if (alertFlags & ALERT_LOW_BP)  return BED_ANGLE_AUTO_LOW_BP;

  return -1;
}

// ---------------------------------------------------------------
// medicationTask: smart medication dosage control (0-100 mg/hr).
// Reads the target from medDosageCommandQueue (dashboard slider)
// and ramps the actual dosage toward it in small steps
// (DOSAGE_RAMP_MG_PER_SEC) so an abrupt slider jump never reaches
// the pump instantly -- this is the "prevent sudden changes" rate
// limit. Three safety levels: normal (<50), warning (50-80), and
// critical (>80). The slider may command the complete 0-100 range;
// while critical, the red LED and buzzer pulse together at
// DOSAGE_BLINK_INTERVAL_MS.
// Publishes the live dosage + safety level for displayTask to show
// on the OLED. Only this task touches MED_LED_PIN/MED_BUZZER_PIN.
// ---------------------------------------------------------------
void medicationTask(void *param) {

  esp_task_wdt_add(NULL);

  pinMode(MED_LED_PIN, OUTPUT);
  pinMode(MED_BUZZER_PIN, OUTPUT);
  digitalWrite(MED_LED_PIN, LOW);
  digitalWrite(MED_BUZZER_PIN, LOW);

  float currentDosage = 0;
  int targetDosage = 0;
  bool blinkOn = false;
  uint32_t lastBlinkToggle = 0;
  const float stepPerTick = DOSAGE_RAMP_MG_PER_SEC * (DOSAGE_TASK_TICK_MS / 1000.0);

  xQueueOverwrite(medDosageCommandQueue, &targetDosage);
  xQueueOverwrite(medDosageStatusQueue, &currentDosage);
  int initialLevel = 0;
  xQueueOverwrite(medDosageLevelQueue, &initialLevel);

  while (1) {

    xQueuePeek(medDosageCommandQueue, &targetDosage, 0);

    int effectiveTarget = constrain(targetDosage, DOSAGE_MIN, DOSAGE_MAX);

    // Rate-limit: never move more than stepPerTick per tick,
    // regardless of how large the requested jump was.
    if (currentDosage < effectiveTarget) {
      currentDosage = min((float)effectiveTarget, currentDosage + stepPerTick);
    } else if (currentDosage > effectiveTarget) {
      currentDosage = max((float)effectiveTarget, currentDosage - stepPerTick);
    }

    int dosageLevel;
    if (currentDosage > DOSAGE_CRITICAL_THRESHOLD) dosageLevel = 2;       // critical
    else if (currentDosage >= DOSAGE_WARNING_THRESHOLD) dosageLevel = 1;  // warning
    else dosageLevel = 0;                                                // normal

    xQueueOverwrite(medDosageStatusQueue, &currentDosage);
    xQueueOverwrite(medDosageLevelQueue, &dosageLevel);

    if (dosageLevel == 2) {
      uint32_t now = millis();
      if (now - lastBlinkToggle >= DOSAGE_BLINK_INTERVAL_MS) {
        blinkOn = !blinkOn;
        digitalWrite(MED_LED_PIN, blinkOn ? HIGH : LOW);
        digitalWrite(MED_BUZZER_PIN, blinkOn ? HIGH : LOW);
        lastBlinkToggle = now;
      }
    } else {
      digitalWrite(MED_LED_PIN, LOW);
      digitalWrite(MED_BUZZER_PIN, LOW);
      blinkOn = false;
    }

    esp_task_wdt_reset();
    vTaskDelay(pdMS_TO_TICKS(DOSAGE_TASK_TICK_MS));
  }
}

// ---------------------------------------------------------------
// bedControlTask: smooth bed-angle servo control. Reads the manual
// target (slider or mode button, via bedCommandQueue), the staff
// override toggle (via bedOverrideQueue), and the patient's current
// condition (via snapshotQueue), and asks computeAutoBedAngle() for
// a clinically recommended angle. If a relevant condition is active
// AND the override toggle is off, the automatic angle overrides the
// manual setting -- patient safety takes priority over whatever
// position was last commanded, by default. Staff can flip the
// override feed to "on" to disable auto-angle entirely and take
// full manual control (e.g. during a procedure or exam). Movement
// always steps one degree at a time (BED_STEP_DELAY_MS apart)
// rather than jumping, satisfying the "prevent sudden large
// movements" safety constraint. Only this task touches the servo;
// only mqttTask touches the mqtt client -- they communicate purely
// via bedCommandQueue / bedStatusQueue / bedOverrideQueue / snapshotQueue.
// ---------------------------------------------------------------
void bedControlTask(void *param) {

  esp_task_wdt_add(NULL);

  int currentAngle = BED_ANGLE_SLEEPING;   // start in a safe, low position
  int manualTarget = BED_ANGLE_SLEEPING;
  int manualOverride = 0;                  // 0 = auto-angle enabled by default
  VitalsSnapshot snap;

  bedServo.write(currentAngle);
  xQueueOverwrite(bedStatusQueue, &currentAngle);
  xQueueOverwrite(bedOverrideQueue, &manualOverride);

  while (1) {

    // Pick up the latest manual command and override state, if any
    // (non-blocking -- keep the last known values if nothing new
    // has arrived).
    xQueuePeek(bedCommandQueue, &manualTarget, 0);
    xQueuePeek(bedOverrideQueue, &manualOverride, 0);

    // Auto-override: map the patient's current condition to a
    // clinically appropriate bed angle. Physiological alerts (SpO2,
    // temp, HR, BP) each map to their own recommended range; two or
    // more distinct categories active at once escalate to the
    // "multiple critical alerts" range. ECG and motion alerts don't
    // have a bed-angle recommendation, so they're ignored here and
    // the manual target is kept. Skipped entirely while staff has
    // the override toggle on.
    int effectiveTarget = manualTarget;
    if (!manualOverride && xQueuePeek(snapshotQueue, &snap, 0) == pdTRUE) {
      int autoAngle = computeAutoBedAngle(snap.alertFlags);
      if (autoAngle >= 0) {
        effectiveTarget = autoAngle;
      }
    }

    effectiveTarget = constrain(effectiveTarget, BED_ANGLE_MIN, BED_ANGLE_MAX);

    // Step smoothly toward the target, one degree per tick -- never
    // jump straight there, even for the emergency override.
    if (currentAngle < effectiveTarget) {
      currentAngle++;
      bedServo.write(currentAngle);
      xQueueOverwrite(bedStatusQueue, &currentAngle);
    } else if (currentAngle > effectiveTarget) {
      currentAngle--;
      bedServo.write(currentAngle);
      xQueueOverwrite(bedStatusQueue, &currentAngle);
    }

    esp_task_wdt_reset();
    vTaskDelay(pdMS_TO_TICKS(BED_STEP_DELAY_MS));
  }
}

void setup() {

  Serial.begin(115200);

  pinMode(BUZZER_PIN_1, OUTPUT);
  pinMode(BUZZER_PIN_2, OUTPUT);
  pinMode(BUZZER_PIN_3, OUTPUT);
  pinMode(BUZZER_PIN_4, OUTPUT);
  pinMode(BUZZER_PIN_5, OUTPUT);
  pinMode(BUZZER_PIN_6, OUTPUT);
  pinMode(BUZZER_PIN_7, OUTPUT);
  pinMode(MOTION_DETECTOR, INPUT);

  bedServo.attach(BED_SERVO_PIN);

  oledMutex = xSemaphoreCreateMutex();
  alertSemaphore = xSemaphoreCreateCounting(10, 0);

  sensors.begin();
  dhtSensor.setup(ENV_DHT_PIN, DHTesp::DHT22);

  Wire.begin(21, 22);

  if (!display.begin(
        SSD1306_SWITCHCAPVCC,
        OLED_ADDRESS)) {

    Serial.println("OLED 1 FAILED");
    while (1);
  }

  if (!display2.begin(
        SSD1306_SWITCHCAPVCC,
        OLED_ADDRESS2)) {

    Serial.println("OLED 2 FAILED");
    while (1);
  }

  xSemaphoreTake(oledMutex, portMAX_DELAY);

  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SSD1306_WHITE);
  display.setCursor(0, 0);
  display.println("OLED OK");
  display.display();

  display2.clearDisplay();
  display2.setTextSize(1);
  display2.setTextColor(SSD1306_WHITE);
  display2.setCursor(0, 0);
  display2.println("OLED OK");
  display2.display();

  xSemaphoreGive(oledMutex);

  delay(2000);

  spiffsReady = SPIFFS.begin(true);
  if (spiffsReady) {
    Serial.println("SPIFFS mounted; persistent offline logging ready");
  } else {
    Serial.println("SPIFFS FAILED; offline persistence unavailable");
  }

  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.println("WiFi connection started (non-blocking)");

  hrQueue       = xQueueCreate(1, sizeof(int));
  spo2Queue     = xQueueCreate(1, sizeof(int));
  tempQueue     = xQueueCreate(1, sizeof(float));
  motionQueue   = xQueueCreate(1, sizeof(int));
  alertQueue    = xQueueCreate(1, sizeof(uint8_t));
  bpQueue       = xQueueCreate(1, sizeof(BPReading));
  ecgQueue      = xQueueCreate(1, sizeof(int));
  envQueue      = xQueueCreate(1, sizeof(EnvReading));
  snapshotQueue = xQueueCreate(1, sizeof(VitalsSnapshot));
  mqttHealthQueue = xQueueCreate(1, sizeof(int));
  systemHealthQueue = xQueueCreate(1, sizeof(int));
  frequencyCommandQueue = xQueueCreate(1, sizeof(int));
  samplingIntervalQueue = xQueueCreate(1, sizeof(int));
  bedCommandQueue  = xQueueCreate(1, sizeof(int));
  bedStatusQueue   = xQueueCreate(1, sizeof(int));
  bedOverrideQueue = xQueueCreate(1, sizeof(int));

  medDosageCommandQueue = xQueueCreate(1, sizeof(int));
  medDosageStatusQueue  = xQueueCreate(1, sizeof(float));
  medDosageLevelQueue   = xQueueCreate(1, sizeof(int));

  // Start at five seconds for the first safety evaluation. Normal
  // conditions then switch to the slider/default interval.
  int defaultFrequency = SAMPLING_DEFAULT_SECONDS;
  int initialSamplingInterval = SAMPLING_MIN_SECONDS;
  xQueueOverwrite(frequencyCommandQueue, &defaultFrequency);
  xQueueOverwrite(samplingIntervalQueue, &initialSamplingInterval);

  int initialMqttHealth = 0;
  int initialSystemHealth = HEALTH_OFFLINE_WIFI;
  xQueueOverwrite(mqttHealthQueue, &initialMqttHealth);
  xQueueOverwrite(systemHealthQueue, &initialSystemHealth);

  mqtt.subscribe(&bedAngleSub);
  mqtt.subscribe(&bedOverrideSub);
  mqtt.subscribe(&bedModeSub);
  mqtt.subscribe(&medDosageSub);
  mqtt.subscribe(&frequencySub);

  // ---- Watchdog: API differs by arduino-esp32 core version.
  // ESP-IDF 5.x cores use the struct-based config API and
  // auto-init the TWDT before setup() runs, so init can fail with
  // "already initialized" -- reconfigure instead if that happens.
  // ESP-IDF 4.x cores (older PlatformIO boards packages) only have
  // the simple two-argument esp_task_wdt_init(timeout, panic). ----
  #if ESP_IDF_VERSION_MAJOR >= 5
    esp_task_wdt_config_t wdtConfig = {
      .timeout_ms = WDT_TIMEOUT_SEC * 1000,
      .idle_core_mask = 0,
      .trigger_panic = true
    };
    esp_err_t wdtResult = esp_task_wdt_init(&wdtConfig);
    if (wdtResult != ESP_OK) {
      esp_task_wdt_reconfigure(&wdtConfig);
    }
  #else
    esp_task_wdt_init(WDT_TIMEOUT_SEC, true);
  #endif

  xTaskCreatePinnedToCore(sensorTask,      "SensorTask",   4096, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(bpTask,          "BPTask",       2048, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(ecgTask,         "ECGTask",      2048, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(environmentTask, "EnvTask",      2048, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(processingTask,  "ProcessTask",  4096, NULL, 2, NULL, 0);
  xTaskCreatePinnedToCore(dataAggregationTask, "AggregateTask", 3072, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(displayTask,     "DisplayTask",  4096, NULL, 2, NULL, 0);
  xTaskCreatePinnedToCore(mqttTask,        "MQTTTask",     6144, NULL, 2, NULL, 0);
  xTaskCreatePinnedToCore(bedControlTask,  "BedTask",      2048, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(medicationTask,  "MedTask",      2048, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(alertTask,       "AlertTask",    2048, NULL, 2, NULL, 0);
  xTaskCreatePinnedToCore(systemHealthTask,"HealthTask",   3072, NULL, 1, NULL, 1);
}

void loop() {
  vTaskDelay(portMAX_DELAY);
}
