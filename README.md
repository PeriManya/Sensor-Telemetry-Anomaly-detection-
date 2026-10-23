# High-Frequency Sensor Telemetry & Anomaly Waveform Analytics

An end-to-end industrial IoT telemetry pipeline designed to process real-time vibrational sensor data. The platform leverages advanced frequency transformations, time-series matrix profile analysis, and statistical profiling to detect mechanical degradation before physical equipment failure occurs.

---

## 🛠️ Tech Stack & Architecture

- **Language:** Python 3.x
- **Streaming & Infrastructure:** Apache Kafka, Zookeeper, Docker
- **Data Analytics & Signal Processing:** NumPy, Pandas, Matplotlib, STUMPY, PyWavelets
- **Machine Learning:** Scikit-learn (Isolation Forest)

---

## 📂 Repository Structure

```text
├── dataset.csv                      # Cleaned telemetry vibration dataset
├── docker-compose.yml               # Kafka & Zookeeper container configuration
├── data_cleaner.py                  # Telemetry cleaning & noise filtration module
├── sensor_streamer.py               # Kafka producer streaming multi-channel telemetry
├── frequency_eda.py                 # Fast Fourier Transform (FFT) frequency analysis
├── matrix_profile_analysis.py       # STUMPY matrix profile motif discovery
├── wavelet_analysis.py              # Continuous Wavelet Transform (CWT) & MRA
├── statistical_baseline.py          # 3-sigma statistical baseline profiling
├── statistical_baseline_results.csv # Baseline statistical output metrics
├── requirements.txt                 # Project dependencies
└── README.md                        # Documentation
```

---

## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone [https://github.com/PeriManya/Sensor-Telemetry-Anomaly-detection.git](https://github.com/PeriManya/Sensor-Telemetry-Anomaly-detection.git)
cd Sensor-Telemetry-Anomaly-detection

### 2. Set Up Virtual Environment & Dependencies
```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On Mac/Linux:
source venv/bin/activate

pip install -r requirements.txt

### 3. Spin Up Streaming Infrastructure
```bash
docker-compose up -d

### 4. Run the Pipeline
```bash
# Start telemetry streamer
python sensor_streamer.py

# Run baseline analysis
python statistical_baseline.py
```

## 📈 Key Features Implemented
- **Ingestion & Preprocessing:** Low-latency streaming setup via Kafka with automated noise filtration.
- **Time-Frequency Diagnostics:** FFT spectral density mapping and multi-resolution wavelet decomposition.
- **Motif Discovery:** Matrix profiling via STUMPY for autonomous pattern recognition.
- **Statistical Profiling:** Automated 3-sigma upper/lower limit threshold generation.
