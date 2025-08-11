import os
import io
import xml.etree.ElementTree as ET
import json
import uuid
import subprocess
import base64
import tempfile
import shutil
import platform
import hashlib
import re
import logging
import zipfile
import sys
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Set, Any
from dataclasses import dataclass, asdict

# Dipendenze opzionali per PDF
try:
    from fpdf import FPDF, XPos, YPos
    from prettytable import PrettyTable
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("⚠️ fpdf2 e prettytable non installate. Funzionalità PDF disabilitate.")
    print("Installa con: pip install fpdf2 prettytable")

# --- SCRIPT INFO ---
SCRIPT_VERSION = "ENHANCED_FATTURE_COMMERCIALISTA_v3.0_INTEGRATO"

# --- CONFIGURAZIONE ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('invoice_processor.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'

# Dizionario mesi italiani
MESI_ITALIANI = {
    1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile", 5: "Maggio", 6: "Giugno",
    7: "Luglio", 8: "Agosto", 9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
}

@dataclass
class ProcessingResult:
    file_name: str
    status: str  # OK, KO, SKIPPED
    method_used: str
    error_message: Optional[str] = None
    company_name: Optional[str] = None
    invoice_year: Optional[str] = None
    hash_md5: Optional[str] = None
    is_duplicate: bool = False
    has_ritenuta: bool = False
    importo_ritenuta: float = 0.0

@dataclass
class FatturaCompleta:
    """Struttura completa fattura con dati ritenute."""
    # Dati base
    file_name: str
    invoice_number: str
    invoice_date: date
    invoice_year: str
    total_amount: float
    
    # Dati anagrafici
    cedente_denominazione: str
    cedente_id_fiscale: str
    cedente_partita_iva: str
    cessionario_denominazione: str
    cessionario_id_fiscale: str
    cessionario_partita_iva: str
    
    # Dati ritenute
    has_ritenuta: bool
    importo_ritenuta: float
    tipo_ritenuta: str
    
    # Metadati elaborazione
    company_name: str
    direction: str
    status: str

# --- CONFIGURAZIONE GLOBALE ---
try:
    from asn1crypto import cms
except ImportError:
    logger.error("ERRORE: asn1crypto non installata")
    exit(1)

win32crypt = None
if platform.system() == "Windows":
    try:
        import win32crypt
    except ImportError:
        logger.warning("pywin32 non disponibile")

PORTFOLIO_AZIENDE: Dict[str, str] = {"02327190845": "VIZZI_GIUSEPPE"}
INPUT_DIR = Path("fatture_da_processare")
OUTPUT_BASE_DIR = Path("aziende_processate")
ARCHIVE_DIR = Path("archivio_input")

# Pattern regex ottimizzati
METADATA_PATTERN = r'(_MT_|[Mm][Ee][Tt][Aa][Dd][Aa][Tt][Oo])'
NOTIFICATION_PATTERN = r'_(?:NS|RC|MC|NE|DT|AT|SE)_'
SUPPORTED_EXTENSIONS = {'.xml', '.p7m'}

# Database duplicati
processed_invoices_db = {}
YEAR_SUBFOLDERS = {'JSON': 'json', 'XML_DECODIFICATI': 'xml_decodificati', 'P7M_ORIGINALI': 'p7m_originali', 'METADATI': 'metadati', 'RICEVUTE': 'ricevute'}

# --- FUNZIONI UTILITY ---
def calculate_hash(file_path: Path) -> str:
    """Calcola hash MD5."""
    try:
        with open(file_path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return f"error_{uuid.uuid4().hex[:8]}"

def is_metadata(file_path: Path) -> bool:
    return bool(re.search(METADATA_PATTERN, file_path.name))

def is_notification(file_path: Path) -> bool:
    return bool(re.search(NOTIFICATION_PATTERN, file_path.name))

def is_supported_file(file_path: Path) -> bool:
    return file_path.suffix.lower() in {'.xml', '.p7m'}

def safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', name)[:200]

def extract_year(date_str: str) -> str:
    if not date_str:
        return "SCONOSCIUTO"
    match = re.search(r'(20\d{2})', date_str)
    return match.group(1) if match else "SCONOSCIUTO"

def truncate_string(value: str, max_length: int = 25) -> str:
    return value if len(value) <= max_length else value[:max_length] + "..."

def get_denominazione_or_nome_cognome(node):
    """Estrae denominazione o nome+cognome da nodo XML."""
    if node is None:
        return "Dati anagrafici mancanti"
    
    denominazione = node.find('Denominazione')
    if denominazione is not None and denominazione.text:
        return denominazione.text.strip()

    nome = node.find('Nome')
    cognome = node.find('Cognome')
    if nome is not None and cognome is not None:
        return f"{nome.text.strip()} {cognome.text.strip()}"

    return "Dati anagrafici mancanti"

# --- ESTRAZIONE P7M OTTIMIZZATA ---
def extract_xml_from_p7m(p7m_path: Path) -> Tuple[Optional[str], str]:
    """Estrae XML da P7M con multiple strategie."""
    strategies = [
        ("Python ASN1", lambda: cms.ContentInfo.load(p7m_path.read_bytes())['content']['encap_content_info']['content'].native.decode('utf-8', errors='ignore')),
        ("Windows API", lambda: win32crypt.CryptDecodeMessage(win32crypt.PKCS_7_ASN_ENCODING | win32crypt.X509_ASN_ENCODING, None, win32crypt.CMSG_SIGNED, p7m_path.read_bytes(), len(p7m_path.read_bytes()))[0].decode('utf-8', errors='ignore') if win32crypt else None),
        ("OpenSSL", lambda: extract_with_openssl(p7m_path))
    ]
    
    for name, func in strategies:
        try:
            result = func()
            if result and '<?xml' in result:
                return result, name
        except Exception:
            continue
    return None, "FAILED"

def extract_with_openssl(p7m_path: Path) -> Optional[str]:
    """Estrazione con OpenSSL."""
    try:
        command = ['openssl', 'cms', '-verify', '-noverify', '-inform', 'DER', '-in', str(p7m_path)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout:
            xml_start = result.stdout.find('<?xml')
            return result.stdout[xml_start:] if xml_start != -1 else None
    except Exception:
        pass
    return None

# --- PARSING XML COMPLETO ---
def parse_invoice_completo(xml_content: str) -> Optional[Tuple[FatturaCompleta, str, str, str, str]]:
    """Parser completo XML con dati ritenute."""
    try:
        # Pulisce XML e rimuove namespace
        clean_xml = xml_content[xml_content.find('<?xml'):]
        it = ET.iterparse(io.StringIO(clean_xml))
        for _, el in it:
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]
        root = it.root
        
        # Funzione helper per estrazione testo
        def get_text(path): 
            elem = root.find(path)
            return elem.text.strip() if elem is not None and elem.text else None
        
        def get_float(path):
            text = get_text(path)
            return float(text.replace(',', '.')) if text else 0.0
        
        # Estrae dati anagrafici
        header = root.find('FatturaElettronicaHeader')
        cedente = header.find('CedentePrestatore')
        cessionario = header.find('CessionarioCommittente')
        body = root.find('FatturaElettronicaBody')
        dati_generali = body.find('DatiGenerali')
        dati_generali_documento = dati_generali.find('DatiGeneraliDocumento')
        
        # Dati cedente
        cedente_id_fiscale = get_text(".//CedentePrestatore/DatiAnagrafici/CodiceFiscale") or "N/D"
        cedente_partita_iva = get_text(".//CedentePrestatore/DatiAnagrafici/IdFiscaleIVA/IdCodice") or "N/D"
        cedente_denominazione = get_denominazione_or_nome_cognome(cedente.find('DatiAnagrafici/Anagrafica'))
        
        # Dati cessionario
        cessionario_id_fiscale = get_text(".//CessionarioCommittente/DatiAnagrafici/CodiceFiscale") or "N/D"
        cessionario_partita_iva = get_text(".//CessionarioCommittente/DatiAnagrafici/IdFiscaleIVA/IdCodice") or "N/D"
        cessionario_denominazione = get_denominazione_or_nome_cognome(cessionario.find('DatiAnagrafici/Anagrafica'))
        
        # Per identificazione portfolio, usa P.IVA o CF come fallback
        sender_vat = cedente_partita_iva if cedente_partita_iva != "N/D" else cedente_id_fiscale
        receiver_vat = cessionario_partita_iva if cessionario_partita_iva != "N/D" else cessionario_id_fiscale
        
        # Determina direzione
        if sender_vat in PORTFOLIO_AZIENDE:
            azienda_vat, direction = sender_vat, "EMESSE"
            partner_vat = receiver_vat
        elif receiver_vat in PORTFOLIO_AZIENDE:
            azienda_vat, direction = receiver_vat, "RICEVUTE"
            partner_vat = sender_vat
        else:
            return None
        
        # Dati generali fattura
        invoice_date_str = get_text(".//DatiGeneraliDocumento/Data")
        invoice_date = datetime.strptime(invoice_date_str, '%Y-%m-%d').date() if invoice_date_str else date.today()
        invoice_year = str(invoice_date.year)
        
        # Dati ritenute
        ritenuta_node = dati_generali_documento.find('DatiRitenuta')
        has_ritenuta = ritenuta_node is not None
        importo_ritenuta = 0.0
        tipo_ritenuta = "N/D"
        
        if has_ritenuta:
            importo_ritenuta = get_float(".//DatiRitenuta/ImportoRitenuta")
            tipo_ritenuta = get_text(".//DatiRitenuta/TipoRitenuta") or "N/D"
        
        # Costruisce oggetto fattura completa
        fattura = FatturaCompleta(
            file_name="",  # Sarà impostato dal chiamante
            invoice_number=get_text(".//DatiGeneraliDocumento/Numero") or "N/D",
            invoice_date=invoice_date,
            invoice_year=invoice_year,
            total_amount=get_float(".//ImportoTotaleDocumento"),
            
            cedente_denominazione=cedente_denominazione,
            cedente_id_fiscale=cedente_id_fiscale,
            cedente_partita_iva=cedente_partita_iva,
            cessionario_denominazione=cessionario_denominazione,
            cessionario_id_fiscale=cessionario_id_fiscale,
            cessionario_partita_iva=cessionario_partita_iva,
            
            has_ritenuta=has_ritenuta,
            importo_ritenuta=importo_ritenuta,
            tipo_ritenuta=tipo_ritenuta,
            
            company_name=PORTFOLIO_AZIENDE[azienda_vat],
            direction=direction,
            status="OK"
        )
        
        return fattura, azienda_vat, direction, get_text(".//DatiGeneraliDocumento/TipoDocumento"), invoice_year
        
    except Exception as e:
        logger.error(f"Errore parsing XML: {e}")
        return None

# --- GESTIONE DUPLICATI ---
def is_duplicate(file_path: Path, xml_content: str) -> Tuple[bool, str]:
    """Controlla duplicati usando hash e contenuto."""
    file_hash = calculate_hash(file_path)
    content_hash = hashlib.sha256(re.sub(r'\s+', '', xml_content).encode()).hexdigest()[:16]
    key = f"{file_hash}_{content_hash}"
    
    if key in processed_invoices_db:
        return True, key
    
    processed_invoices_db[key] = {'file': file_path.name, 'processed_at': datetime.now().isoformat()}
    return False, key

# --- ORGANIZZAZIONE FILE ---
def create_structure(company: str, year: str, direction: str) -> Path:
    """Crea struttura directory."""
    base_dir = OUTPUT_BASE_DIR / company / direction / year
    for subfolder in YEAR_SUBFOLDERS.values():
        (base_dir / subfolder).mkdir(parents=True, exist_ok=True)
    return base_dir

def find_related_files(invoice_path: Path) -> Dict[str, List[Path]]:
    """Trova file correlati."""
    base_name = invoice_path.stem
    parent_dir = invoice_path.parent
    related = {'metadata': [], 'notifications': []}
    
    for file_path in parent_dir.iterdir():
        if file_path.is_file() and file_path.stem.startswith(base_name):
            if is_metadata(file_path):
                related['metadata'].append(file_path)
            elif is_notification(file_path):
                related['notifications'].append(file_path)
    
    return related

def save_organized_files(file_path: Path, xml_content: str, fattura: FatturaCompleta, method: str, original_content: bytes = None) -> bool:
    """Salva file organizzati."""
    try:
        base_dir = create_structure(fattura.company_name, fattura.invoice_year, fattura.direction)
        base_name = safe_filename(file_path.stem)
        related_files = find_related_files(file_path)
        
        # Costruisce risultato completo
        result = {
            "id": str(uuid.uuid4()),
            "fileName": file_path.name,
            "company": {"name": fattura.company_name, "vatId": ""},
            "direction": fattura.direction,
            "invoiceData": {
                "invoiceNumber": fattura.invoice_number,
                "invoiceDate": fattura.invoice_date.isoformat(),
                "invoiceYear": fattura.invoice_year,
                "totalAmount": fattura.total_amount,
                "partner": {
                    "name": fattura.cessionario_denominazione if fattura.direction == "EMESSE" else fattura.cedente_denominazione,
                    "vatId": fattura.cessionario_partita_iva if fattura.direction == "EMESSE" else fattura.cedente_partita_iva
                }
            },
            "ritenuta": {
                "has_ritenuta": fattura.has_ritenuta,
                "importo_ritenuta": fattura.importo_ritenuta,
                "tipo_ritenuta": fattura.tipo_ritenuta
            },
            "anagrafici": {
                "cedente": {
                    "denominazione": fattura.cedente_denominazione,
                    "partita_iva": fattura.cedente_partita_iva,
                    "codice_fiscale": fattura.cedente_id_fiscale
                },
                "cessionario": {
                    "denominazione": fattura.cessionario_denominazione,
                    "partita_iva": fattura.cessionario_partita_iva,
                    "codice_fiscale": fattura.cessionario_id_fiscale
                }
            },
            "processedAt": datetime.now(timezone.utc).isoformat(),
            "processing": {
                "method_used": method,
                "file_hash": calculate_hash(file_path),
                "original_filename": file_path.name
            },
            "related_files": {
                "metadata": [f.name for f in related_files['metadata']],
                "notifications": [f.name for f in related_files['notifications']]
            }
        }
        
        # Salva JSON
        json_path = base_dir / YEAR_SUBFOLDERS['JSON'] / f"{base_name}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        # Salva XML decodificato
        xml_path = base_dir / YEAR_SUBFOLDERS['XML_DECODIFICATI'] / f"{base_name}.xml"
        with open(xml_path, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        
        # Salva originale se necessario
        if is_supported_file(file_path):
            orig_path = base_dir / YEAR_SUBFOLDERS['P7M_ORIGINALI'] / f"{base_name}{file_path.suffix}"
            if original_content:
                with open(orig_path, 'wb') as f:
                    f.write(original_content)
            else:
                shutil.copy2(file_path, orig_path)
        
        # Salva file correlati
        for i, metadata_file in enumerate(related_files['metadata']):
            meta_path = base_dir / YEAR_SUBFOLDERS['METADATI'] / metadata_file.name
            shutil.copy2(metadata_file, meta_path)
            
        for i, notif_file in enumerate(related_files['notifications']):
            notif_path = base_dir / YEAR_SUBFOLDERS['RICEVUTE'] / notif_file.name
            shutil.copy2(notif_file, notif_path)
        
        # Aggiorna prima nota
        update_prima_nota(fattura.company_name, fattura.invoice_year, fattura.direction, result)
        
        logger.info(f"✅ {Colors.GREEN}Organizzato: {fattura.company_name}/{fattura.invoice_year}/{fattura.direction}/{base_name}{Colors.RESET}")
        if fattura.has_ritenuta:
            logger.info(f"   💰 Ritenuta: {fattura.importo_ritenuta:.2f}€ ({fattura.tipo_ritenuta})")
        
        return True
        
    except Exception as e:
        logger.error(f"Errore salvataggio {file_path.name}: {e}")
        return False

def update_prima_nota(company: str, year: str, direction: str, invoice_data: Dict):
    """Aggiorna prima nota anno."""
    try:
        prima_nota_path = OUTPUT_BASE_DIR / company / direction / year / f"prima_nota_{year}.json"
        
        if prima_nota_path.exists():
            with open(prima_nota_path, 'r', encoding='utf-8') as f:
                prima_nota = json.load(f)
        else:
            prima_nota = {
                'company': company, 
                'year': year, 
                'direction': direction, 
                'invoices': [], 
                'summary': {
                    'total_invoices': 0, 
                    'total_amount': 0.0,
                    'total_ritenute': 0.0,
                    'fatture_con_ritenuta': 0
                }
            }
        
        prima_nota['invoices'].append({
            'id': invoice_data['id'],
            'invoice_number': invoice_data['invoiceData']['invoiceNumber'],
            'invoice_date': invoice_data['invoiceData']['invoiceDate'],
            'partner_name': invoice_data['invoiceData']['partner']['name'],
            'total_amount': invoice_data['invoiceData']['totalAmount'],
            'has_ritenuta': invoice_data['ritenuta']['has_ritenuta'],
            'importo_ritenuta': invoice_data['ritenuta']['importo_ritenuta'],
            'added_at': datetime.now(timezone.utc).isoformat()
        })
        
        # Calcola summary
        invoices = prima_nota['invoices']
        prima_nota['summary'] = {
            'total_invoices': len(invoices),
            'total_amount': sum(inv['total_amount'] for inv in invoices),
            'total_ritenute': sum(inv['importo_ritenuta'] for inv in invoices if inv['has_ritenuta']),
            'fatture_con_ritenuta': sum(1 for inv in invoices if inv['has_ritenuta']),
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        with open(prima_nota_path, 'w', encoding='utf-8') as f:
            json.dump(prima_nota, f, ensure_ascii=False, indent=2)
            
    except Exception as e:
        logger.error(f"Errore prima nota {company}/{year}: {e}")

# --- PROCESSAMENTO PRINCIPALE ---
def process_file(file_path: Path, is_temp: bool = False) -> Tuple[ProcessingResult, Optional[FatturaCompleta]]:
    """Processa singolo file."""
    result = ProcessingResult(file_name=file_path.name, status="KO", method_used="NONE")
    
    try:
        original_content = file_path.read_bytes() if not is_temp else None
        
        # Estrae XML
        if file_path.suffix.lower() == '.p7m':
            xml_content, method = extract_xml_from_p7m(file_path)
            if not xml_content:
                result.error_message = "Estrazione P7M fallita"
                return result, None
        else:
            xml_content = file_path.read_text(encoding='utf-8', errors='ignore')
            method = "DIRECT_XML"
        
        result.method_used = method
        
        # Parse fattura
        parse_result = parse_invoice_completo(xml_content)
        if not parse_result:
            result.error_message = "Parsing fallito o fattura non del portfolio"
            return result, None
        
        fattura, azienda_vat, direction, tipo_doc, year = parse_result
        fattura.file_name = file_path.name
        
        result.company_name = fattura.company_name
        result.invoice_year = fattura.invoice_year
        result.hash_md5 = calculate_hash(file_path)
        result.has_ritenuta = fattura.has_ritenuta
        result.importo_ritenuta = fattura.importo_ritenuta
        
        # Controlla duplicati
        is_dup, dup_key = is_duplicate(file_path, xml_content)
        if is_dup:
            result.status = "SKIPPED"
            result.is_duplicate = True
            result.error_message = f"Duplicato: {dup_key}"
            return result, fattura
        
        # Salva organizzato
        if save_organized_files(file_path, xml_content, fattura, method, original_content):
            result.status = "OK"
        else:
            result.error_message = "Errore salvataggio"
            
        return result, fattura
        
    except Exception as e:
        result.error_message = f"Errore: {e}"
        return result, None

def should_skip(file_path: Path) -> Tuple[bool, str]:
    """Determina se saltare file."""
    if is_metadata(file_path):
        return False, "METADATA"
    if is_notification(file_path):
        return True, "NOTIFICATION"
    if not is_supported_file(file_path):
        return True, "UNSUPPORTED"
    return False, "INVOICE"

# --- ANALISI RITENUTE ---
def filter_fatture_by_date_and_ritenuta(fatture: List[FatturaCompleta], start_date: Optional[date], end_date: Optional[date], only_ritenute: bool = False) -> List[FatturaCompleta]:
    """Filtra fatture per data e ritenute."""
    filtered = []
    for fattura in fatture:
        # Filtro ritenute
        if only_ritenute and not fattura.has_ritenuta:
            continue
            
        # Filtro date
        if start_date and fattura.invoice_date < start_date:
            continue
        if end_date and fattura.invoice_date > end_date:
            continue
            
        filtered.append(fattura)
    
    return filtered

def aggregate_by_supplier_and_client(fatture: List[FatturaCompleta]) -> Dict[str, Dict[int, List[FatturaCompleta]]]:
    """Aggrega fatture per fornitore e mese."""
    aggregato = {}
    for fattura in fatture:
        fornitore = fattura.cedente_denominazione
        mese = fattura.invoice_date.month
        
        if fornitore not in aggregato:
            aggregato[fornitore] = {}
        if mese not in aggregato[fornitore]:
            aggregato[fornitore][mese] = []
            
        aggregato[fornitore][mese].append(fattura)
    
    return aggregato

# --- GENERAZIONE PDF ---
def export_to_pdf(fatture: List[FatturaCompleta], pdf_file_path: str, start_date: Optional[date], end_date: Optional[date]):
    """Genera report PDF con riepiloghi ritenute."""
    if not PDF_AVAILABLE:
        logger.warning("PDF non disponibile - librerie mancanti")
        return
        
    try:
        pdf = FPDF(orientation="L", unit="mm", format="A4")
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.set_font("Helvetica", size=10)

        # Intestazione
        pdf.add_page()
        pdf.set_font("Helvetica", style="B", size=12)
        pdf.cell(0, 10, f"{SCRIPT_VERSION} - Report Fatture con Ritenute", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        pdf.ln(1)

        # Periodo
        if start_date and end_date:
            periodo = f"Periodo: dal {start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}"
            pdf.cell(0, 10, periodo, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
            pdf.ln(1)

        oggi = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        grouped_fatture = aggregate_by_supplier_and_client(fatture)

        # Riepilogo ritenute
        pdf.set_font("Helvetica", style="B", size=12)
        pdf.cell(0, 10, "Riepilogo Ritenute per Fornitore:", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        pdf.set_font("Helvetica", size=10)
        pdf.ln(1)

        for fornitore, fatture_per_mese in grouped_fatture.items():
            # Calcola totale periodo per fornitore
            fatture_fornitore = [f for f in fatture if f.cedente_denominazione == fornitore]
            totale_periodo = sum(f.importo_ritenuta for f in fatture_fornitore if f.has_ritenuta)
            
            # Prende prima fattura per CF/P.IVA
            prima_fattura = fatture_fornitore[0] if fatture_fornitore else None
            cf_piva = prima_fattura.cedente_partita_iva if prima_fattura else "N/D"

            pdf.set_font("Helvetica", style="B", size=10)
            pdf.cell(0, 10, f"Fornitore: {fornitore} (P.IVA/C.F.: {cf_piva})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
            pdf.set_font("Helvetica", size=10)
            pdf.cell(0, 10, f"Totale Ritenute: €{totale_periodo:.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
            pdf.ln(1)

            # Dettaglio per mese
            for mese, fatture_gruppo in fatture_per_mese.items():
                totale_mese = sum(f.importo_ritenuta for f in fatture_gruppo if f.has_ritenuta)
                if totale_mese > 0:
                    pdf.cell(0, 10, f"  {MESI_ITALIANI[mese]}: €{totale_mese:.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")

        # Dettaglio fatture per fornitore
        for fornitore, fatture_per_mese in grouped_fatture.items():
            pdf.add_page()
            pdf.set_font("Helvetica", style="B", size=10)
            pdf.cell(0, 10, f"DETTAGLIO: {fornitore.upper()}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
            pdf.set_font("Helvetica", size=8)
            pdf.cell(0, 10, f"Generato: {oggi}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")

            # Intestazioni tabella
            headers = ["#", "File", "Cliente", "Numero", "Data", "Ritenuta", "Importo €", "Status"]
            col_widths = [10, 50, 60, 25, 25, 15, 25, 30]

            pdf.ln()
            pdf.set_font("Helvetica", style="B", size=8)
            for header, width in zip(headers, col_widths):
                pdf.cell(width, 8, header, border=1, align="C")
            pdf.ln()

            # Dati fatture
            pdf.set_font("Helvetica", size=7)
            idx = 1
            for mese, fatture_gruppo in fatture_per_mese.items():
                fatture_gruppo.sort(key=lambda x: x.invoice_date)
                for fattura in fatture_gruppo:
                    data = [
                        str(idx),
                        truncate_string(fattura.file_name, 20),
                        truncate_string(fattura.cessionario_denominazione, 25),
                        fattura.invoice_number,
                        fattura.invoice_date.strftime('%d/%m/%Y'),
                        "SÌ" if fattura.has_ritenuta else "NO",
                        f"{fattura.importo_ritenuta:.2f}" if fattura.has_ritenuta else "-",
                        fattura.status
                    ]
                    for value, width in zip(data, col_widths):
                        pdf.cell(width, 8, str(value), border=1, align="C")
                    pdf.ln()
                    idx += 1

        pdf.output(pdf_file_path)
        logger.info(f"📄 PDF generato: {pdf_file_path}")
        
    except Exception as e:
        logger.error(f"Errore generazione PDF: {e}")

# --- REPORT OTTIMIZZATO ---
def generate_report(results: List[ProcessingResult], fatture: List[FatturaCompleta]) -> str:
    """Genera report finale con statistiche ritenute."""
    total = len(results)
    ok_count = sum(1 for r in results if r.status == "OK")
    ko_count = sum(1 for r in results if r.status == "KO")
    skip_count = sum(1 for r in results if r.status == "SKIPPED")
    
    # Statistiche ritenute
    fatture_con_ritenuta = sum(1 for f in fatture if f and f.has_ritenuta)
    totale_ritenute = sum(f.importo_ritenuta for f in fatture if f and f.has_ritenuta)
    
    companies = set(r.company_name for r in results if r.company_name)
    years = set(r.invoice_year for r in results if r.invoice_year)
    methods = {}
    for r in results:
        if r.method_used != "NONE":
            methods[r.method_used] = methods.get(r.method_used, 0) + 1
    
    report = [
        f"\n{Colors.BOLD}{Colors.CYAN}{'='*70}",
        f"REPORT ELABORAZIONE - {SCRIPT_VERSION}",
        f"{'='*70}{Colors.RESET}",
        f"\n📊 STATISTICHE GENERALI:",
        f"  • File totali: {total}",
        f"  • Elaborati OK: {Colors.GREEN}{ok_count}{Colors.RESET}",
        f"  • Errori: {Colors.RED}{ko_count}{Colors.RESET}",
        f"  • Saltati: {Colors.YELLOW}{skip_count}{Colors.RESET}",
        f"  • Tasso successo: {Colors.GREEN}{(ok_count/total*100):.1f}%{Colors.RESET}" if total > 0 else "",
        f"\n💰 STATISTICHE RITENUTE:",
        f"  • Fatture con ritenuta: {Colors.YELLOW}{fatture_con_ritenuta}{Colors.RESET}",
        f"  • Totale ritenute: {Colors.YELLOW}€{totale_ritenute:.2f}{Colors.RESET}",
        f"  • Media ritenuta: €{(totale_ritenute/fatture_con_ritenuta):.2f}" if fatture_con_ritenuta > 0 else "",
        f"\n🔧 METODI DECODIFICA:"
    ]
    
    for method, count in methods.items():
        report.append(f"  • {method}: {count}")
    
    if companies:
        report.append(f"\n🏢 AZIENDE: {', '.join(companies)}")
    if years:
        report.append(f"📅 ANNI: {', '.join(sorted(years))}")
    
    # Tabella riassuntiva con ritenute
    report.extend([
        f"\n📋 DETTAGLIO ELABORAZIONE:",
        f"{'FILE':<35} {'STATUS':<8} {'METODO':<12} {'RITENUTA':<8} {'IMPORTO':<10} {'ERRORE':<25}",
        "-" * 100
    ])
    
    for r, f in zip(results, [f for f in fatture] + [None] * (len(results) - len(fatture))):
        status_color = Colors.GREEN if r.status == "OK" else Colors.YELLOW if r.status == "SKIPPED" else Colors.RED
        status = f"{status_color}{r.status}{Colors.RESET}"
        ritenuta = "SÌ" if f and f.has_ritenuta else "NO" if f else "-"
        importo = f"€{f.importo_ritenuta:.2f}" if f and f.has_ritenuta else "-"
        error = (r.error_message[:24] if r.error_message else "")
        
        report.append(f"{r.file_name[:34]:<35} {status:<18} {r.method_used[:11]:<12} {ritenuta:<8} {importo:<10} {error:<25}")
    
    return "\n".join(report)

def print_ritenute_summary(fatture: List[FatturaCompleta], start_date: Optional[date], end_date: Optional[date]):
    """Stampa riepilogo ritenute organizzato."""
    if not PDF_AVAILABLE:
        return
        
    try:
        aggregato = aggregate_by_supplier_and_client(fatture)
        
        print(f"\n{Colors.BOLD}{Colors.CYAN}RIEPILOGO RITENUTE PER FORNITORE:{Colors.RESET}")
        
        for fornitore, fatture_per_mese in aggregato.items():
            fatture_fornitore = [f for f in fatture if f.cedente_denominazione == fornitore]
            totale_periodo = sum(f.importo_ritenuta for f in fatture_fornitore if f.has_ritenuta)
            
            if totale_periodo > 0:
                print(f"\n{Colors.BOLD}📊 {fornitore}{Colors.RESET}")
                print(f"   Totale periodo: {Colors.YELLOW}€{totale_periodo:.2f}{Colors.RESET}")
                
                for mese, fatture_gruppo in sorted(fatture_per_mese.items()):
                    totale_mese = sum(f.importo_ritenuta for f in fatture_gruppo if f.has_ritenuta)
                    if totale_mese > 0:
                        print(f"   {MESI_ITALIANI[mese]}: €{totale_mese:.2f}")
                        
    except Exception as e:
        logger.error(f"Errore riepilogo ritenute: {e}")

# --- MAIN INTEGRATO ---
def main():
    logger.info(f"--- {SCRIPT_VERSION} ---")
    
    # Parsing argomenti linea comando
    if len(sys.argv) < 2:
        print(f"Sintassi: python {sys.argv[0]} [cartella_input] [-R] [data_inizio data_fine]")
        print("Esempio: python script.py fatture_da_processare -R 01/01/2024 31/12/2024")
        print("-R: filtra solo fatture con ritenuta")
        return
    
    # Configurazione da argomenti o default
    if len(sys.argv) >= 2 and Path(sys.argv[1]).exists():
        global INPUT_DIR
        INPUT_DIR = Path(sys.argv[1])
    
    filter_ritenute = '-R' in sys.argv
    start_date = None
    end_date = None
    
    # Parse date se presenti
    argv_clean = [arg for arg in sys.argv if arg != '-R']
    if len(argv_clean) >= 4:
        try:
            start_date = datetime.strptime(argv_clean[2], '%d/%m/%Y').date()
            end_date = datetime.strptime(argv_clean[3], '%d/%m/%Y').date()
        except ValueError:
            print("Formato date non valido. Usa DD/MM/YYYY")
            return
    
    try:
        # Crea directory
        for directory in [INPUT_DIR, OUTPUT_BASE_DIR, ARCHIVE_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
        
        if not INPUT_DIR.exists():
            raise Exception(f"Directory input mancante: {INPUT_DIR}")
        
        # Raccoglie file
        invoice_files = []
        results = []
        fatture_complete = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Scansiona file e ZIP
            for file_path in INPUT_DIR.rglob('*'):
                if not file_path.is_file():
                    continue
                
                if file_path.suffix.lower() == '.zip':
                    try:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            extract_dir = temp_path / file_path.stem
                            zip_ref.extractall(extract_dir)
                            
                            for extracted in extract_dir.rglob('*'):
                                if extracted.is_file():
                                    skip, file_type = should_skip(extracted)
                                    if not skip and file_type == "INVOICE":
                                        invoice_files.append((extracted, True))
                        
                        # Archivia ZIP
                        zip_archive = ARCHIVE_DIR / "zip_processati"
                        zip_archive.mkdir(exist_ok=True)
                        shutil.move(file_path, zip_archive / file_path.name)
                        
                    except Exception as e:
                        logger.error(f"Errore ZIP {file_path.name}: {e}")
                else:
                    skip, file_type = should_skip(file_path)
                    if not skip and file_type == "INVOICE":
                        invoice_files.append((file_path, False))
            
            logger.info(f"📄 Trovate {len(invoice_files)} fatture")
            
            # Processa fatture
            for file_path, is_temp in invoice_files:
                result, fattura = process_file(file_path, is_temp)
                results.append(result)
                
                if fattura:
                    fatture_complete.append(fattura)
                
                # Archivia se successo e non temporaneo
                if result.status == "OK" and not is_temp:
                    try:
                        archive_path = ARCHIVE_DIR / file_path.relative_to(INPUT_DIR)
                        archive_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(file_path, archive_path)
                    except Exception as e:
                        logger.error(f"Errore archiviazione {file_path.name}: {e}")
        
        # Filtra fatture se richiesto
        fatture_filtrate = filter_fatture_by_date_and_ritenuta(fatture_complete, start_date, end_date, filter_ritenute)
        
        # Report finale
        report = generate_report(results, fatture_complete)
        print(report)
        
        # Riepilogo ritenute
        if fatture_filtrate:
            print_ritenute_summary(fatture_filtrate, start_date, end_date)
        
        # Genera PDF se richiesto e disponibile
        if fatture_filtrate and PDF_AVAILABLE:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pdf_name = f"report_ritenute_{timestamp}.pdf"
            pdf_path = OUTPUT_BASE_DIR / pdf_name
            export_to_pdf(fatture_filtrate, str(pdf_path), start_date, end_date)
        
        # Salva report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = OUTPUT_BASE_DIR / f"report_{timestamp}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(re.sub(r'\033\[\d+m', '', report))
        
        logger.info(f"📄 Report salvato: {report_path}")
        
        # Summary finale
        total_ritenute = sum(f.importo_ritenuta for f in fatture_filtrate if f.has_ritenuta)
        print(f"\n{Colors.BOLD}🎯 SUMMARY FINALE:{Colors.RESET}")
        print(f"   • Fatture elaborate: {len(results)}")
        print(f"   • Fatture con ritenuta: {len([f for f in fatture_filtrate if f.has_ritenuta])}")
        print(f"   • Totale ritenute: {Colors.YELLOW}€{total_ritenute:.2f}{Colors.RESET}")
        
    except Exception as e:
        logger.critical(f"ERRORE FATALE: {e}")

if __name__ == "__main__":
    main()