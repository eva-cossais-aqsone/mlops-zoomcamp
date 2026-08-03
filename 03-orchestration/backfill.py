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
                flow_run_name=f"backfill-run-from-{start_date.strftime('%Y-%m')}-to-{end_date.strftime('%Y-%m')}"
            )
            custom_scheduled_run(execution_date=current_date)
        except Exception as e:
            print(f"Erreur pour {current_date.strftime('%Y-%m')}: {e}")

        current_date += relativedelta(months=1)


if __name__ == "__main__":
    start = datetime(2025, 1, 1)
    end = datetime(2025, 3, 1)
    backfill(start, end)
