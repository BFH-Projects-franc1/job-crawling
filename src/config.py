import os
import random
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# === BASE DIRECTORY SETUP === #
BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DATA_FOLDER = os.path.join(BASE_DIR, "data")
HTML_FOLDER = os.path.join(DATA_FOLDER, "html")

# === SCRAPER CONFIGURATION === #
NUM_JOBS = 1000  # Number of job listings to scrape
HTML_FILE_LIMIT = 90  # Max number of HTML files to save
SCRAPER_THREADS = 3  # Adjust based on system capabilities
FETCHER_THREADS = 3  # Adjust based on system performance & website limitations
BATCH_SIZE = 25  # Jobs to fetch per request
MAX_RETRIES = 3  # Retry limit for failed requests
# Job titles to search for in the webpage to get better and more data.
# -> move your favorite Job-title to the first position as it will be fetched and scraped first.
JOB_TITLES = [
    "Elektroniker", "Informatiker", "Arzt", "Buchhalter", "Ingenieur", "Mechaniker",
    "Lehrer", "Verkäufer", "Koch", "Krankenpfleger", "Zahnarzt", "Rechtsanwalt",
    "Architekt", "Bankkaufmann", "Bauleiter", "Bürokaufmann", "Chemiker", "Dachdecker",
    "Designer", "Diplomkaufmann", "Dreher", "Einzelhandelskaufmann", "Ergotherapeut",
    "Erzieher", "Fachinformatiker", "Flugbegleiter", "Friseur", "Gärtner", "Gebäudereiniger",
    "Geologe", "Grafiker", "Hausmeister", "Heilpraktiker", "Hotelfachmann", "Immobilienmakler",
    "Industriekaufmann", "Ingenieur für Maschinenbau", "Installateur", "Journalist",
    "Kälteanlagenbauer", "Kameramann", "Kapitän", "Kassierer", "Kfz-Mechatroniker",
    "Kindergärtner", "Klempner", "Konditor", "Kosmetiker", "Kriminalbeamter", "Lackierer",
    "Landwirt", "Logistiker", "Luftverkehrskaufmann", "Maler", "Masseur", "Mediengestalter",
    "Meeresbiologe", "Metzger", "Möbeltischler", "Musiker", "Notar", "Obstbauer",
    "Optiker", "Orthopäde", "Pädagoge", "Pilot", "Polizist", "Postbote", "Programmierer",
    "Psychologe", "Radiologe", "Reinigungskraft", "Restaurantfachmann", "Sanitäter",
    "Schneider", "Schornsteinfeger", "Schreiner", "Sozialarbeiter", "Spezialtiefbauer",
    "Speditionskaufmann", "Sportlehrer", "Steuerberater", "Strassenbauer", "Systemadministrator",
    "Tänzer", "Technischer Zeichner", "Tierarzt", "Tischler", "Tontechniker", "Übersetzer",
    "Uhrmacher", "Umweltingenieur", "Veranstaltungstechniker", "Verfahrensmechaniker",
    "Verkehrsplaner", "Versicherungskaufmann", "Verwaltungsfachangestellter",
    "Weintechnologe", "Werkzeugmechaniker", "Wirtschaftsprüfer", "Zimmermann"
]

# === API CONFIGURATION === #
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY not found in environment variables.")

# === USER-AGENTS (ROTATION) === #
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/13.1.2 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0"
]

# === HEADERS === #
HEADERS = {"User-Agent": random.choice(USER_AGENTS)}
