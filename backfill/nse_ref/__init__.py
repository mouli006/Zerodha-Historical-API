"""NSE delisting, suspension and merger reference data (nse_reference.py), 2015 to today.

Sources (verified 2026-10-14):
- Circulars: GET https://www.nseindia.com/api/circulars?fromDate=DD-MM-YYYY&toDate=DD-MM-YYYY returns
  {"data": [...]} for every department (no department or keyword filter needed; a whole year came back
  uncapped, 6,819 rows for 2024). Row fields: cirDate (YYYYMMDD), circDisplayNo (NSE/CML/55885),
  circDepartment ("Listing", "Emerge", ...), sub, circFilelink (nsearchives .../CML55885.pdf). It answers
  scripts without cookies; the www.nseindia.com home page itself answers them with 403 (ignored).
- Suspension circulars of securities come from Listing and Emerge (file dept CML); Inspection &
  Compliance publishes "Suspension of Member" (brokers) and Mutual Fund its scheme suspensions, which
  are skipped with the other non-equity subjects (bonds, NCDs, units, revocations...).
- Delisting of Companies: https://www.nseindia.com/static/list/list-of-companies-proposed-to-be-delisted
  links two .xlsx files ("List of Companies Delisted from NSE", "List of Companies Proposed to be
  Delisted") whose URLs carry a new timestamp on every update.
- Orders of compulsory delisting: https://www.nseindia.com/static/list/orders-of-delisting, a static
  HTML table DATE | SUBJECT (company) | LINK (order PDF), 2016 onwards; old order PDFs are scans.
- Merger suspension circulars carry the old symbol, the dates and "Scheme of Amalgamation" but almost
  never the new company or the swap ratio, and the corporate-actions API only says "Merger": those
  come from reference/manual_mergers.csv when the circular lacks them.
"""
