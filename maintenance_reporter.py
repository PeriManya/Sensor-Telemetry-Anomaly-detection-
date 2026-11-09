import pandas as pd
from datetime import datetime

def generate_maintenance_report(input_csv="extracted_features.csv", report_output="maintenance_report.txt"):
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        print(f"Error reading dataset: {e}")
        return

    if "anomaly_flag" in df.columns:
        critical_events = df[df["anomaly_flag"] == 1]
    else:
        critical_events = pd.DataFrame()
    
    report_content = []
    report_content.append("="*50)
    report_content.append("    AUTOMATED MAINTENANCE INCIDENT REPORT")
    report_content.append(f"    Generated On: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_content.append("="*50 + "\n")
    
    report_content.append(f"Total Sensor Windows Processed: {len(df)}")
    report_content.append(f"Total Anomalous Events Flagged: {len(critical_events)}\n")
    
    if not critical_events.empty:
        report_content.append("--- AT-RISK MACHINERY DETAILS ---")
        for idx, row in critical_events.iterrows():
            window_id = row.get("window", idx)
            peak_amp = row.get("peak_amplitude", "N/A")
            freq = row.get("dominant_frequency", "N/A")
            report_content.append(
                f"[CRITICAL ALERT] Window #{window_id} | Peak Amplitude: {peak_amp} | Frequency: {freq} Hz"
            )
    else:
        report_content.append("[STATUS: OK] All equipment operating within normal parameters.")

    report_text = "\n".join(report_content)
    
    # Explicitly use utf-8 encoding for file saving
    with open(report_output, "w", encoding="utf-8") as f:
        f.write(report_text)
        
    print(f"Maintenance report successfully generated at: {report_output}")
    print("Report generated with no encoding errors!")

if __name__ == "__main__":
    generate_maintenance_report()