from dagster import HourlyPartitionsDefinition, DailyPartitionsDefinition


DAILY_START_DATE = '2024-01-01'
DAILY_END_DATE = '2024-01-12'

Daily_partition = DailyPartitionsDefinition(
    start_date=DAILY_START_DATE,
    end_date=DAILY_END_DATE
)

# hourly_partition = HourlyPartitionsDefinition(
#     start_date=START_DATE,
#     end_date=END_DATE
# )