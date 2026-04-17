<img width="1919" height="631" alt="image" src="https://github.com/user-attachments/assets/4d77049d-c5a6-4a68-bc1d-53f7702317f9" />
<img width="1168" height="313" alt="image" src="https://github.com/user-attachments/assets/f62dcafc-96af-4b1b-b15b-62b2f6b55ccd" />
# CS2 Market Analytics & Arbitrage Engine v2.0

##  Project Overview
This project is a high-performance **Decision Support System (DSS)** designed to identify and exploit **Information Asymmetry (Market Lag)** between the Steam Community Market (SCM) and high-liquidity external marketplaces (e.g., Skinport). 

Due to Valve's 7-day Trade Hold policy, the system focuses on **Time-Series Analysis** and **Trend Prediction** rather than high-frequency execution, allowing users to build a profitable inventory based on real-time data discrepancies.

##  System Architecture
The system is built on a modular, 3-tier architecture ensuring clean separation of concerns:

1.  **The Harvester (Ingestion Layer):** An autonomous background worker that polls multi-market APIs every 30 minutes. It handles data sanitization, currency conversion, and respects platform-specific rate limits.
2.  **Database Manager (Persistence Layer):** An optimized SQLite3 time-series database. It utilizes **B-Tree indexing** and **As-Of Join** logic to ensure data continuity even during system downtime.
3.  **Analytics Terminal (Presentation Layer):** A high-density Fin-Tech dashboard built with Streamlit and Plotly, providing real-time visualization of market "Lags" and "Pumps".

##  Key Technical Challenges & Resilience
Building a reliable pipeline in the restricted CS2 ecosystem required solving several enterprise-level engineering problems:

*   **HTTP 429 (Rate Limiting):** Navigated Steam's aggressive anti-spam measures by implementing a custom **Exponential Backoff** algorithm with random jitter.
*   **HTTP 403 (Cloudflare Bypassing):** Overcame strict TLS fingerprinting on external markets using `curl_cffi` for browser impersonation (Chrome 120) and implemented **Brotli decompression** to handle high-density JSON payloads.
*   **Data Integrity:** Developed robust sanitization pipelines to handle "dirty" API data, including Unicode non-breaking spaces (`\xa0`) and localized currency formatting.

##  Core Features
*   **Rolling Window Deltas:** Real-time calculation of price changes in 3h, 12h, and 24h windows using advanced SQL queries.
*   **Market Lag Indicator:** Identifies when external "brain" markets move before the Steam "tail" market.
*   **Paper Trading Module:** A dedicated "Spiritual Portfolio" that tracks simulated buys, accounting for 15% platform fees to calculate **Real Net ROI**.
*   **Multimodal AI Readiness:** Designed to integrate with Gemini 2.0 Flash for automated sentiment analysis of unstructured market reports.

##  Installation & Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/konradL1337/CS2-Arbitrage-Terminal.git
   Install dependencies:
pip install -r requirements.txt
Configure environment:

Create a .env file based on .env.example.

Add your GEMINI_API_KEY and SKINPORT credentials.

Run the components:

Start the engine: python harvester.py

Launch the dashboard: streamlit run app.py
