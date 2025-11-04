Problem:
    1. Interday data fetching results in wrong csv: ,^GSPC,^GSPC,^GSPC,^GSPC,^GSPC,^GSPC
    2. Always start server up not on demand - this is to slow and first request will fail
    3. Return Adj Close for historical data
    4. Fix error: KeyError: 'Date' in @app.get("/history")