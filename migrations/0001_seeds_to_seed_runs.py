from sqlite_utils import Database
import sys


def main():
    db_name = sys.argv[1]
    db = Database(db_name)
    db["_seed_runs"].create(
        {
            "run_repr": str,
            "seed_name": str,
            "beaker_name": str,
            "num_items": int,
            "start_time": str,
            "end_time": str,
        },
        pk="run_repr",
        if_not_exists=True,
    )
    for row in db["_seeds"].rows:
        db["_seed_runs"].insert(
            {
                "run_repr": f"sr:{row['name']}",
                "seed_name": row["name"],
                "beaker_name": row["beaker_name"],
                "num_items": row["num_items"],
                "start_time": row["imported_at"],
                "end_time": row["imported_at"],
            }
        )


if __name__ == "__main__":
    main()
