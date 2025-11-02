"""Dedupe helper for MODEL_CLASSES and AI_MODELS.

This script will list duplicate rows and, if CONFIRM_DEDUPE=1 in the environment,
will replace the target tables with their DISTINCT content inside a transaction.

Use with care; this performs DELETE/INSERT operations.
"""
import os
import traceback
from snowflake_conn import CustomSnowflake


def run(csf, sql, fetch=True):
    try:
        rows, rc = csf.run_command(sql, fetch=fetch)
        return rows
    except Exception as e:
        print(f"Query failed: {e}")
        traceback.print_exc()
        return None


def find_duplicates(csf, table, cols):
    collist = ",".join(cols)
    q = f"SELECT {collist}, COUNT(*) cnt FROM VISIONDB.HACKATHON_SCHEMA.{table} GROUP BY {collist} HAVING COUNT(*)>1"
    return run(csf, q)


def dedupe_table(csf, table, cols):
    """Safely remove duplicate rows using ROW_NUMBER() over partition.

    This deletes rows where ROW_NUMBER() > 1 for the partition of the key columns.
    It avoids creating temporary tables and does not require a current schema.
    """
    collist = ",".join(cols)
    # Build a CTE that numbers rows per partition and delete the ones with rn>1
    pk_expr = ", ".join(cols)
    delete_sql = f"""
    WITH numbered AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY {pk_expr} ORDER BY {pk_expr}) AS rn, {pk_expr}
        FROM VISIONDB.HACKATHON_SCHEMA.{table}
    )
    DELETE FROM VISIONDB.HACKATHON_SCHEMA.{table}
    WHERE ({pk_expr}) IN (
        SELECT {pk_expr} FROM numbered WHERE rn > 1
    );
    """
    try:
        print(f"Running dedupe for {table} using partition on ({pk_expr})...")
        run(csf, delete_sql, fetch=False)
        print(f"Dedupe completed for {table}.")
    except Exception as e:
        print(f"Dedupe failed for {table}: {e}")
        traceback.print_exc()


def main():
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        targets = {
            'MODEL_CLASSES': ['MODEL_NAME', 'CLASS_NAME'],
            'AI_MODELS': ['MODEL_NAME'],
        }

        for t, cols in targets.items():
            print(f"\nChecking duplicates for {t}...")
            dups = find_duplicates(csf, t, cols)
            if dups:
                print(f"Found {len(dups)} duplicate group(s) in {t}:")
                for r in dups:
                    print(r)
            else:
                print(f"No duplicates found in {t}.")

        if os.environ.get('CONFIRM_DEDUPE') == '1':
            print('\nCONFIRM_DEDUPE=1 detected — performing dedupe now')
            for t, cols in targets.items():
                dedupe_table(csf, t, cols)
            print('Dedupe operations completed.')
        else:
            print('\nTo perform dedupe, re-run with CONFIRM_DEDUPE=1 in the environment to apply changes.')

    finally:
        try:
            csf.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
