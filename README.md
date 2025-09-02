# TAREFA-Dev
tarefa passada para teste para dev


import time
import zipfile
import logging
import csv
from datetime import datetime, timedelta
import os
import psycopg2

LOG_PATH = "./logs/execucao.log"
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# Configurações
FILENAME_TEMPLATE = "rel_encaminhamentos_cmc_regional_{date}.zip"
WATCH_DIR = os.path.expanduser(r"C:/prodam") #Só o diretorio(caminho da pasta) para puxar o arquivo
TARGET_TABLE = "public.carga_diaria"

PGHOST = "localhost"
PGPORT = 5432
PGDATABASE ="carga_db"
PGUSER ="postgres"
PGPASSWORD = "123456"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_csv_from_zip(zip_path):
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.endswith(".csv"):
                z.extract(name, WATCH_DIR)
                logger.info(f"Arquivo CSV extraído: {name}")
                return os.path.join(WATCH_DIR, name)
    logger.warning("Nenhum arquivo CSV encontrado no ZIP.")
    return None

def load_csv_to_db(csv_file):
    import psycopg2
    import csv

    logger.info(f"Tentando abrir o arquivo CSV: {csv_file}")
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD
    )
    cur = conn.cursor()
    logger.info("Esvaziando tabela antes da carga...")
    cur.execute(f"TRUNCATE TABLE {TARGET_TABLE};")
    with open(csv_file, encoding="cp1252") as f:
        reader = csv.reader(f)
        next(reader)  # Pula o cabeçalho, se houver
        for row in reader:
            cur.execute(
                f"INSERT INTO {TARGET_TABLE} (coluna1, coluna2, coluna3, data_carga) VALUES (%s, %s, %s, %s);",
                (*row, datetime.now().date())
            )
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Carga concluída com sucesso.")

def process_range(start_date: datetime, end_date: datetime):
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        filename = f"rel_encaminhamentos_cmc_regional_{date_str}.csv"
        filepath = os.path.join(WATCH_DIR, filename)
        logger.info(f"Procurando arquivo: {filepath}")
        if os.path.exists(filepath):
            logger.info("Arquivo CSV encontrado!")
            load_csv_to_db(filepath)
        else:
            logger.warning(f"Arquivo CSV não encontrado para a data {current_date.strftime('%d/%m/%Y')}.")
        current_date += timedelta(days=1)
if __name__ == "__main__":
    # Datas de 23/08/2025 a 29/08/2025
    start = datetime(2025,8,23)
    end = datetime(2025,8,29)
    process_range(start, end)


def build_attempt_schedule(today: datetime) -> list[datetime]:
    """
    Retorna os timestamps de tentativas entre 03:00 e 06:00 inclusivo, a cada 30 minutos.
    """
    base = today.replace(hour=3, minute=0, second=0, microsecond=0)
    end = today.replace(hour=6, minute=0, second=0, microsecond=0)
    times = []
    current = base
    while current <= end:
        times.append(current)
        current += timedelta(minutes=30)
    return times

def wait_until(ts: datetime):
    """
    Aguarda (sleep) até o instante ts, se necessário.
    """
    now = datetime.now()
    if ts > now:
        seconds = (ts - now).total_seconds()
        logger.info(f"Aguardando até {ts.strftime('%H:%M')} (~{int(seconds)}s)")
        time.sleep(seconds)

