from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import csv
import io
#input csv files
INPATIENT_CSV = "/opt/airflow/data/csv_data/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv"
BENEFICIARY_CSV = "/opt/airflow/data/csv_data/DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv"

POSTGRES_CONN_ID = "postgres_local"  
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="patient_claims_plus_postgres",
    default_args=default_args,
    description="Postgres version of patient_claims_plus (claims + beneficiary join, ICD fields)",
    schedule_interval=None,
    start_date=datetime(2024, 11, 1),
    catchup=False,
)

start = EmptyOperator(task_id="start", dag=dag)


def load_csv_subset_to_table(csv_path: str, table: str, keep_cols, **context):

    #Reads CSV from the Airflow container and streams selected columns into Postgres table
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [c for c in keep_cols if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"Missing columns in {csv_path}: {missing}")

        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(keep_cols)

        for row in reader:
            w.writerow([row.get(c, "") for c in keep_cols])

        buf.seek(0)

        copy_sql = f"COPY {table} ({', '.join(keep_cols)}) FROM STDIN WITH (FORMAT csv, HEADER true)"
        cur.copy_expert(copy_sql, buf)

    conn.commit()
    cur.close()
    conn.close()

#dropping final tabel if it already exists
drop_final_table_if_exists = PostgresOperator(
    task_id="drop_final_table_if_exists",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="DROP TABLE IF EXISTS patient_claims_plus;",
    dag=dag,
)
#creating raw staging tables
#Postgres uses unquoted colum names to lowercase whereas SQL references may appear uppercase
create_raw_tables = PostgresOperator(
    task_id="create_raw_tables",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    DROP TABLE IF EXISTS raw_inpatient_claims;
    DROP TABLE IF EXISTS raw_beneficiary;

    CREATE TABLE raw_inpatient_claims (
      desynpuf_id TEXT,
      clm_from_dt TEXT,
      clm_thru_dt TEXT,
      clm_id TEXT,
      prvdr_num TEXT,
      clm_pmt_amt NUMERIC,
      icd9_dgns_cd_1 TEXT,
      icd9_dgns_cd_2 TEXT,
      icd9_dgns_cd_3 TEXT,
      icd9_dgns_cd_4 TEXT,
      icd9_dgns_cd_5 TEXT,
      icd9_dgns_cd_6 TEXT,
      icd9_dgns_cd_7 TEXT,
      icd9_dgns_cd_8 TEXT,
      icd9_dgns_cd_9 TEXT
    );

    CREATE TABLE raw_beneficiary (
      desynpuf_id TEXT,
      bene_hi_cvrage_tot_mons INT,
      bene_smi_cvrage_tot_mons INT,
      bene_birth_dt TEXT,
      bene_death_dt TEXT,
      bene_sex_ident_cd INT
    );
    """,
    dag=dag,
)

#loading inpatient claims csv into raw table
load_raw_inpatient_claims_from_csv = PythonOperator(
    task_id="load_raw_inpatient_claims_from_csv",
    python_callable=load_csv_subset_to_table,
    op_kwargs={
        "csv_path": INPATIENT_CSV,
        "table": "raw_inpatient_claims",
        "keep_cols": [
            "DESYNPUF_ID", "CLM_FROM_DT", "CLM_THRU_DT", "CLM_ID", "PRVDR_NUM", "CLM_PMT_AMT",
            "ICD9_DGNS_CD_1", "ICD9_DGNS_CD_2", "ICD9_DGNS_CD_3", "ICD9_DGNS_CD_4",
            "ICD9_DGNS_CD_5", "ICD9_DGNS_CD_6", "ICD9_DGNS_CD_7", "ICD9_DGNS_CD_8",
            "ICD9_DGNS_CD_9",
        ],
    },
    dag=dag,
)

dq_rowcount_raw_inpatient = PostgresOperator(
    task_id="dq_rowcount_raw_inpatient",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS ok FROM raw_inpatient_claims;",
    dag=dag,
)
#load beneficiary csv into raw table
load_raw_beneficiary_from_csv = PythonOperator(
    task_id="load_raw_beneficiary_from_csv",
    python_callable=load_csv_subset_to_table,
    op_kwargs={
        "csv_path": BENEFICIARY_CSV,
        "table": "raw_beneficiary",
        "keep_cols": [
            "DESYNPUF_ID",
            "BENE_HI_CVRAGE_TOT_MONS",
            "BENE_SMI_CVRAGE_TOT_MONS",
            "BENE_BIRTH_DT",
            "BENE_DEATH_DT",
            "BENE_SEX_IDENT_CD",
        ],
    },
    dag=dag,
)

dq_rowcount_raw_beneficiary = PostgresOperator(
    task_id="dq_rowcount_raw_beneficiary",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS ok FROM raw_beneficiary;",
    dag=dag,
)
#Transforming - creating joined patient_claims_plus table
create_patient_claims_plus = PostgresOperator(
    task_id="create_patient_claims_plus",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    CREATE TABLE patient_claims_plus AS
    SELECT
      a.DESYNPUF_ID AS patient_id,
      a.CLM_FROM_DT AS claim_from_date,
      a.CLM_THRU_DT AS claim_thru_date,
      a.CLM_ID AS claim_id,
      a.PRVDR_NUM AS provider_number,
      a.CLM_PMT_AMT AS claim_payment_amount,
      a.ICD9_DGNS_CD_1 AS icd_diagnosis_code_1,
      a.ICD9_DGNS_CD_2 AS icd_diagnosis_code_2,
      a.ICD9_DGNS_CD_3 AS icd_diagnosis_code_3,
      a.ICD9_DGNS_CD_4 AS icd_diagnosis_code_4,
      a.ICD9_DGNS_CD_5 AS icd_diagnosis_code_5,
      a.ICD9_DGNS_CD_6 AS icd_diagnosis_code_6,
      a.ICD9_DGNS_CD_7 AS icd_diagnosis_code_7,
      a.ICD9_DGNS_CD_8 AS icd_diagnosis_code_8,
      a.ICD9_DGNS_CD_9 AS icd_diagnosis_code_9,
      b.BENE_HI_CVRAGE_TOT_MONS AS patient_hospital_insurance_total_months,
      b.BENE_SMI_CVRAGE_TOT_MONS AS patient_supplementary_medical_insurance_total_months,
      b.BENE_BIRTH_DT AS patient_birth_date,
      b.BENE_DEATH_DT AS patient_death_date,
      CASE
        WHEN b.BENE_SEX_IDENT_CD = 1 THEN 'Male'
        WHEN b.BENE_SEX_IDENT_CD = 2 THEN 'Female'
        ELSE 'Unknown'
      END AS patient_sex
    FROM raw_inpatient_claims a
    LEFT JOIN raw_beneficiary b
      ON a.DESYNPUF_ID = b.DESYNPUF_ID;
    """,
    dag=dag,
)
#Data Quality Check: To ensure final table is non-empty
dq_rowcount_patient_claims_plus = PostgresOperator(
    task_id="dq_rowcount_patient_claims_plus",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS ok FROM patient_claims_plus;",
    dag=dag,
)

ready = EmptyOperator(task_id="ready", dag=dag)

(
    start
    >> drop_final_table_if_exists
    >> create_raw_tables
    >> load_raw_inpatient_claims_from_csv
    >> dq_rowcount_raw_inpatient
    >> load_raw_beneficiary_from_csv
    >> dq_rowcount_raw_beneficiary
    >> create_patient_claims_plus
    >> dq_rowcount_patient_claims_plus
    >> ready
)
