#!/usr/bin/env python
import importlib
from datetime import datetime

from dateutil.relativedelta import relativedelta

scheduled_module = importlib.import_module("scheduled-duration-prediction")
scheduled_run = scheduled_module.scheduled_run


def backfill(start_date: datetime, end_date: datetime):
    """Exécute séquentiellement le workflow pour chaque mois entre start_date et end_date."""
    current_date = start_date

    while current_date <= end_date:
        try:
            custom_scheduled_run = scheduled_run.with_options(
                name=f"HW3 - STEP 5 : backfill {start_date.strftime('%Y-%m')}_{end_date.strftime('%Y-%m')}",
                flow_run_name=f"run-execution-date-{current_date.year:04d}-{current_date.month:02d}",
            )
            custom_scheduled_run(execution_date=current_date)
        except Exception as e:
            print(f"Erreur pour {current_date.strftime('%Y-%m')}: {e}")

        current_date += relativedelta(months=1)


if __name__ == "__main__":
    start = datetime(2025, 1, 1)
    end = datetime(2025, 3, 1)
    backfill(start, end)
