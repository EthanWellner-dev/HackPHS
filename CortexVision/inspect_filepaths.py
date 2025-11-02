from snowflake_conn import CustomSnowflake


def main():
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        rows, _ = csf.run_command("SELECT DISTINCT SPLIT_PART(FILE_PATH, '/', 2) AS part2, COUNT(*) cnt FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA GROUP BY part2 ORDER BY cnt DESC", fetch=True)
        print('Distinct part2 values and counts:')
        if rows:
            for r in rows[:50]:
                print(r)

        # Show some raw FILE_PATHs where part2 looks like a filename pattern (contains underscore and .jpg)
        rows, _ = csf.run_command("SELECT FILE_PATH, CAPTION FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA WHERE SPLIT_PART(FILE_PATH, '/', 2) LIKE '%.jpg' OR SPLIT_PART(FILE_PATH, '/', 2) LIKE '%_%' LIMIT 20", fetch=True)
        print('\nExamples where split part looks suspicious:')
        if rows:
            for r in rows:
                print(r)
    finally:
        try:
            csf.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
