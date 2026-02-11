from app.db import init_db
from app.cdc import upsert_source_and_emit

def main():
    init_db()
    upsert_source_and_emit("cases", "CASE-001", "Cannot reset password; MFA issue")
    upsert_source_and_emit("cases", "CASE-002", "Login loop; browser cache suspected")
    upsert_source_and_emit("kb", "KB-001", "Reset password procedure")
    upsert_source_and_emit("kb", "KB-002", "Troubleshoot MFA errors")
    print("Seeded source + CDC events. Start indexer: python -m app.indexer")

if __name__ == "__main__":
    main()
