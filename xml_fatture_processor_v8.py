#!/usr/bin/env python3
"""
SISTEMA INTEGRATO FATTURE ELETTRONICHE AdE - VERSIONE 4.0 CON DECODIFICA AVANZATA
Integrazione completa dei sistemi di decodifica multi-strategia
Sviluppato da Salvatore Crapanzano 

Caratteristiche principali:
- Download completo da portale AdE  
- Decodifica P7M multi-algoritmo avanzata (ASN1, Windows API, OpenSSL)
- Organizzazione CLIENTE/TIPO/ANNO ottimizzata
- Supporto completo ricevute SDI e metadati
- Gestione avanzata duplicati e hash
- Sistema di logging professionale
"""

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import json
import os
import sys
import re
import time
import shutil
import subprocess
import hashlib
import xml.etree.ElementTree as ET
import io
import uuid
import tempfile
import platform
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Any
import logging
from dataclasses import dataclass, asdict
import pytz
from tqdm import tqdm
from urllib.parse import unquote
import argparse

# Dipendenze avanzate per decodifica P7M
try:
    from asn1crypto import cms
    ASN1_AVAILABLE = True
except ImportError:
    ASN1_AVAILABLE = False
    print("AVVISO: asn1crypto non installata. Alcune funzionalità di decodifica P7M ridotte.")

win32crypt = None
if platform.system() == "Windows":
    try:
        import win32crypt
        WIN32_AVAILABLE = True
    except ImportError:
        WIN32_AVAILABLE = False
        print("AVVISO: pywin32 non disponibile su Windows.")
else:
    WIN32_AVAILABLE = False

# --- CONFIGURAZIONE ---
SCRIPT_VERSION = "SISTEMA_INTEGRATO_ADE_v4.0_ADVANCED_DECODER"
CONFIG_FILE = "config_ade_system.json"
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Pattern ottimizzati per riconoscimento file
METADATA_PATTERN = r'(_MT_|[Mm][Ee][Tt][Aa][Dd][Aa][Tt][Oo])'
NOTIFICATION_PATTERN = r'_(?:NS|RC|MC|NE|DT|AT|SE)_'
SUPPORTED_EXTENSIONS = {'.xml', '.p7m'}

# --- DATACLASSES POTENZIATE ---
@dataclass
class AdvancedProcessingResult:
    """Risultato elaborazione con informazioni dettagliate su decodifica."""
    file_name: str
    status: str  # OK, KO, SKIPPED
    method_used: str
    error_message: Optional[str] = None
    company_name: Optional[str] = None
    invoice_year: Optional[str] = None
    hash_md5: Optional[str] = None
    hash_sha256: Optional[str] = None
    is_duplicate: bool = False
    has_ritenuta: bool = False
    importo_ritenuta: float = 0.0
    file_type: Optional[str] = None  # INVOICE, METADATA, NOTIFICATION
    decoding_attempts: List[str] = None
    original_size: int = 0
    decoded_size: int = 0

    def __post_init__(self):
        if self.decoding_attempts is None:
            self.decoding_attempts = []

@dataclass
class FatturaMetadata:
    nome_file_originale: str
    id_file: str
    hash_sha256: str
    tipo_fattura: str
    data_emissione: Optional[str] = None
    data_ricezione: Optional[str] = None
    anno_riferimento: Optional[int] = None
    partita_iva_cedente: Optional[str] = None
    partita_iva_cessionario: Optional[str] = None
    codice_fiscale_cedente: Optional[str] = None
    codice_fiscale_cessionario: Optional[str] = None
    ha_ritenuta: bool = False
    ha_cassa_previdenza: bool = False
    importo_ritenuta: float = 0.0
    tipo_ritenuta: Optional[str] = None
    ricevute_sdi: List[str] = None
    stato_decodifica: str = "non_processato"
    errori_decodifica: List[str] = None
    timestamp_elaborazione: str = None
    metodo_decodifica: Optional[str] = None
    tentativi_decodifica: List[str] = None

    def __post_init__(self):
        if self.ricevute_sdi is None:
            self.ricevute_sdi = []
        if self.errori_decodifica is None:
            self.errori_decodifica = []
        if self.tentativi_decodifica is None:
            self.tentativi_decodifica = []
        if self.timestamp_elaborazione is None:
            self.timestamp_elaborazione = datetime.now().isoformat()

@dataclass
class DownloadResult:
    success: bool
    file_path: Optional[Path] = None
    metadata_path: Optional[Path] = None
    ricevute_sdi_paths: List[Path] = None
    error_message: Optional[str] = None
    file_type: Optional[str] = None
    client_id: Optional[str] = None

    def __post_init__(self):
        if self.ricevute_sdi_paths is None:
            self.ricevute_sdi_paths = []

@dataclass
class OrganizationResult:
    success: bool
    organized_files: int = 0
    decoded_files: int = 0
    errors: List[str] = None
    client_folders_created: Dict[str, List[str]] = None
    decoding_stats: Dict[str, int] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.client_folders_created is None:
            self.client_folders_created = {}
        if self.decoding_stats is None:
            self.decoding_stats = {}

# --- UTILITY FUNCTIONS POTENZIATE ---
def unix_timestamp():
    return str(int(datetime.now(tz=pytz.utc).timestamp() * 1000))

def calculate_file_hash(file_path: Path, algorithm: str = 'sha256') -> str:
    """Calcola hash del file con algoritmo specificato."""
    if algorithm == 'md5':
        hash_algo = hashlib.md5()
    elif algorithm == 'sha256':
        hash_algo = hashlib.sha256()
    else:
        raise ValueError(f"Algoritmo hash non supportato: {algorithm}")
    
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_algo.update(chunk)
        return hash_algo.hexdigest()
    except Exception:
        return ""

def calculate_content_hash(content: str) -> str:
    """Calcola hash del contenuto normalizzato."""
    normalized = re.sub(r'\s+', '', content)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]

def is_metadata(file_path: Path) -> bool:
    """Verifica se il file è un metadato."""
    return bool(re.search(METADATA_PATTERN, file_path.name))

def is_notification(file_path: Path) -> bool:
    """Verifica se il file è una ricevuta/notifica."""
    return bool(re.search(NOTIFICATION_PATTERN, file_path.name))

def is_supported_file(file_path: Path) -> bool:
    """Verifica se il file è supportato."""
    return file_path.suffix.lower() in SUPPORTED_EXTENSIONS

def determine_file_type(file_path: Path) -> str:
    """Determina il tipo di file."""
    if is_metadata(file_path):
        return "METADATA"
    elif is_notification(file_path):
        return "NOTIFICATION"
    elif is_supported_file(file_path):
        return "INVOICE"
    else:
        return "UNSUPPORTED"

def safe_filename(name: str) -> str:
    """Crea nome file sicuro."""
    return re.sub(r'[<>:"/\\|?*]', '_', name)[:200]

def extract_date_from_xml(xml_file_path: Path, file_type: str) -> Tuple[Optional[str], Optional[int]]:
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}')[1]
        
        data_emissione = None
        anno_riferimento = None
        
        for data_elem in root.iter('Data'):
            if data_elem.text:
                data_emissione = data_elem.text
                try:
                    anno_riferimento = int(data_emissione[:4])
                except:
                    pass
                break
        
        if not anno_riferimento:
            anno_riferimento = datetime.now().year
        
        return data_emissione, anno_riferimento
        
    except Exception:
        return None, datetime.now().year

def extract_partita_iva_from_xml(xml_file_path: Path) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}')[1]
        
        piva_cedente = None
        piva_cessionario = None
        cf_cedente = None
        cf_cessionario = None
        
        for cedente in root.iter('CedentePrestatore'):
            for dati in cedente.iter('DatiAnagrafici'):
                for piva in dati.iter('IdFiscaleIVA'):
                    for codice in piva.iter('IdCodice'):
                        piva_cedente = codice.text
                        break
                for cf in dati.iter('CodiceFiscale'):
                    cf_cedente = cf.text
                    break
        
        for cessionario in root.iter('CessionarioCommittente'):
            for dati in cessionario.iter('DatiAnagrafici'):
                for piva in dati.iter('IdFiscaleIVA'):
                    for codice in piva.iter('IdCodice'):
                        piva_cessionario = codice.text
                        break
                for cf in dati.iter('CodiceFiscale'):
                    cf_cessionario = cf.text
                    break
        
        return piva_cedente, piva_cessionario, cf_cedente, cf_cessionario
        
    except Exception:
        return None, None, None, None

def check_ritenuta_cassa_from_xml(xml_file_path: Path) -> Tuple[bool, bool, float, str]:
    """Verifica presenza ritenute e cassa previdenziale con importi."""
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}')[1]
        
        ha_ritenuta = False
        ha_cassa = False
        importo_ritenuta = 0.0
        tipo_ritenuta = "N/D"
        
        # Cerca ritenute
        for elem in root.iter():
            if 'ritenuta' in elem.tag.lower() or 'Ritenuta' in elem.tag:
                ha_ritenuta = True
                break
        
        # Cerca cassa previdenziale
        for elem in root.iter():
            if 'cassa' in elem.tag.lower() or 'Cassa' in elem.tag or 'previdenza' in elem.tag.lower():
                ha_cassa = True
                break
        
        # Estrae importo ritenuta
        if ha_ritenuta:
            for elem in root.iter('ImportoRitenuta'):
                if elem.text:
                    try:
                        importo_ritenuta = float(elem.text.replace(',', '.'))
                        break
                    except:
                        pass
            
            for elem in root.iter('TipoRitenuta'):
                if elem.text:
                    tipo_ritenuta = elem.text
                    break
        
        return ha_ritenuta, ha_cassa, importo_ritenuta, tipo_ritenuta
        
    except Exception:
        return False, False, 0.0, "N/D"

def divide_in_trimestri(data_iniziale: str, data_finale: str) -> List[Tuple[str, str]]:
    def aggiusta_fine_trimestre(d: datetime) -> datetime:
        if d.month < 4:
            return datetime(d.year, 3, 31)
        elif d.month < 7:
            return datetime(d.year, 6, 30)
        elif d.month < 10:
            return datetime(d.year, 9, 30)
        else:
            return datetime(d.year, 12, 31)

    d1 = datetime.strptime(data_iniziale, "%d%m%Y")
    d2 = datetime.strptime(data_finale, "%d%m%Y")
    trimestri = []

    while d1 <= d2:
        fine_trimestre = aggiusta_fine_trimestre(d1)
        if fine_trimestre >= d2:
            trimestri.append((d1.strftime("%d%m%Y"), d2.strftime("%d%m%Y")))
            break
        else:
            trimestri.append((d1.strftime("%d%m%Y"), fine_trimestre.strftime("%d%m%Y")))
        d1 = fine_trimestre + timedelta(days=1)

    return trimestri

def _parse_filename_from_content_disposition(header_val: str) -> Optional[str]:
    if not header_val:
        return None
    
    m_star = re.search(r"filename\*\s*=\s*([^']*)'[^']*'([^;]+)", header_val, flags=re.IGNORECASE)
    if m_star:
        try:
            raw = m_star.group(2)
            decoded = unquote(raw)
            return decoded.strip('"')
        except Exception:
            pass
    
    m = re.search(r'filename\s*=\s*"([^"]+)"', header_val, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    
    m2 = re.search(r'filename\s*=\s*([^;]+)', header_val, flags=re.IGNORECASE)
    if m2:
        return m2.group(1).strip().strip('"')
    
    return None

def determine_file_type_from_path(file_path: Path) -> str:
    path_str = str(file_path).lower()
    
    if 'emesse' in path_str:
        if 'transfrontalier' in path_str:
            return 'transfrontaliere_emesse'
        return 'emesse'
    elif 'ricevute' in path_str:
        if 'transfrontalier' in path_str:
            return 'transfrontaliere_ricevute'
        return 'ricevute'
    elif 'passive' in path_str:
        return 'ricevute'
    elif 'transfrontalier' in path_str:
        return 'transfrontaliere_ricevute'
    else:
        return 'ricevute'

# --- SISTEMA DECODIFICA P7M AVANZATO ---
class AdvancedP7MDecoder:
    """Decodificatore P7M multi-strategia con supporto ASN1, Windows API e OpenSSL."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.stats = {
            'ASN1_SUCCESS': 0,
            'WINDOWS_API_SUCCESS': 0,
            'OPENSSL_SUCCESS': 0,
            'FAILED': 0
        }
    
    def extract_xml_from_p7m_asn1(self, p7m_content: bytes) -> Tuple[Optional[str], List[str]]:
        """Decodifica P7M usando ASN1Crypto."""
        errors = []
        
        if not ASN1_AVAILABLE:
            errors.append("ASN1Crypto non disponibile")
            return None, errors
        
        try:
            content_info = cms.ContentInfo.load(p7m_content)
            
            # Metodo 1: estrazione diretta contenuto
            try:
                content = content_info['content']['encap_content_info']['content'].native
                if isinstance(content, bytes):
                    xml_content = content.decode('utf-8', errors='ignore')
                else:
                    xml_content = str(content)
                
                if xml_content and '<?xml' in xml_content:
                    self.stats['ASN1_SUCCESS'] += 1
                    return xml_content, errors
            except Exception as e:
                errors.append(f"ASN1 metodo 1: {str(e)}")
            
            # Metodo 2: estrazione ricorsiva
            try:
                def extract_content_recursive(obj):
                    if hasattr(obj, 'native') and obj.native:
                        content = obj.native
                        if isinstance(content, bytes):
                            try:
                                decoded = content.decode('utf-8', errors='ignore')
                                if '<?xml' in decoded:
                                    return decoded
                            except:
                                pass
                        elif isinstance(content, str) and '<?xml' in content:
                            return content
                    
                    if hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
                        try:
                            for item in obj:
                                result = extract_content_recursive(item)
                                if result:
                                    return result
                        except:
                            pass
                    
                    if hasattr(obj, '__dict__'):
                        for attr_name in dir(obj):
                            if not attr_name.startswith('_'):
                                try:
                                    attr_value = getattr(obj, attr_name)
                                    result = extract_content_recursive(attr_value)
                                    if result:
                                        return result
                                except:
                                    pass
                    
                    return None
                
                xml_content = extract_content_recursive(content_info)
                if xml_content:
                    self.stats['ASN1_SUCCESS'] += 1
                    return xml_content, errors
                
            except Exception as e:
                errors.append(f"ASN1 metodo 2: {str(e)}")
        
        except Exception as e:
            errors.append(f"ASN1 generale: {str(e)}")
        
        return None, errors
    
    def extract_xml_from_p7m_windows(self, p7m_content: bytes) -> Tuple[Optional[str], List[str]]:
        """Decodifica P7M usando Windows Crypto API."""
        errors = []
        
        if not WIN32_AVAILABLE:
            errors.append("Windows Crypto API non disponibile")
            return None, errors
        
        try:
            # Metodo 1: decodifica standard
            try:
                decoded_bytes, cert_info = win32crypt.CryptDecodeMessage(
                    win32crypt.PKCS_7_ASN_ENCODING | win32crypt.X509_ASN_ENCODING,
                    None,
                    win32crypt.CMSG_SIGNED,
                    p7m_content,
                    len(p7m_content)
                )
                
                xml_content = decoded_bytes.decode('utf-8', errors='ignore')
                if xml_content and '<?xml' in xml_content:
                    self.stats['WINDOWS_API_SUCCESS'] += 1
                    return xml_content, errors
                
            except Exception as e:
                errors.append(f"Windows API metodo 1: {str(e)}")
            
            # Metodo 2: verifica senza controlli
            try:
                decoded_bytes = win32crypt.CryptDecodeMessage(
                    win32crypt.PKCS_7_ASN_ENCODING,
                    None,
                    0,  # Nessun controllo specifico
                    p7m_content,
                    len(p7m_content)
                )[0]
                
                xml_content = decoded_bytes.decode('utf-8', errors='ignore')
                if xml_content and '<?xml' in xml_content:
                    self.stats['WINDOWS_API_SUCCESS'] += 1
                    return xml_content, errors
                
            except Exception as e:
                errors.append(f"Windows API metodo 2: {str(e)}")
        
        except Exception as e:
            errors.append(f"Windows API generale: {str(e)}")
        
        return None, errors
    
    def extract_xml_from_p7m_openssl(self, p7m_path: Path) -> Tuple[Optional[str], List[str]]:
        """Decodifica P7M usando OpenSSL."""
        errors = []
        
        # Lista di comandi OpenSSL da provare
        commands = [
            ['openssl', 'cms', '-verify', '-noverify', '-inform', 'DER', '-in', str(p7m_path)],
            ['openssl', 'cms', '-decrypt', '-verify', '-inform', 'DER', '-in', str(p7m_path), '-noverify'],
            ['openssl', 'smime', '-verify', '-noverify', '-inform', 'DER', '-in', str(p7m_path)],
            ['openssl', 'smime', '-decrypt', '-inform', 'DER', '-in', str(p7m_path), '-noverify'],
            ['openssl', 'cms', '-verify', '-inform', 'PEM', '-in', str(p7m_path), '-noverify'],
            ['openssl', 'cms', '-decrypt', '-inform', 'PEM', '-in', str(p7m_path), '-noverify']
        ]
        
        for i, command in enumerate(commands):
            try:
                result = subprocess.run(
                    command, 
                    capture_output=True, 
                    text=True, 
                    timeout=60,
                    check=False
                )
                
                if result.returncode == 0 and result.stdout:
                    xml_start = result.stdout.find('<?xml')
                    if xml_start != -1:
                        xml_content = result.stdout[xml_start:]
                        if xml_content:
                            self.stats['OPENSSL_SUCCESS'] += 1
                            return xml_content, errors
                
                # Prova anche stderr in caso di output misto
                if result.stderr:
                    xml_start = result.stderr.find('<?xml')
                    if xml_start != -1:
                        xml_content = result.stderr[xml_start:]
                        if xml_content:
                            self.stats['OPENSSL_SUCCESS'] += 1
                            return xml_content, errors
                
            except subprocess.TimeoutExpired:
                errors.append(f"OpenSSL comando {i+1}: timeout")
            except Exception as e:
                errors.append(f"OpenSSL comando {i+1}: {str(e)}")
        
        return None, errors
    
    def decrypt_p7m_file(self, input_file: Path, output_dir: Path) -> Tuple[bool, Optional[Path], List[str], str]:
        """Decodifica file P7M con approccio multi-strategia."""
        if not input_file.name.lower().endswith('.p7m'):
            return False, None, ["Non è un file P7M"], "NONE"
        
        all_errors = []
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_name = input_file.name[:-4] if input_file.name.endswith('.p7m') else input_file.stem
        output_file = output_dir / output_name
        
        try:
            p7m_content = input_file.read_bytes()
            original_size = len(p7m_content)
            
            # Strategia 1: ASN1Crypto
            xml_content, asn1_errors = self.extract_xml_from_p7m_asn1(p7m_content)
            if xml_content:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(xml_content)
                self.logger.debug(f"Decodifica ASN1 riuscita per {input_file.name}")
                return True, output_file, all_errors, "ASN1"
            all_errors.extend(asn1_errors)
            
            # Strategia 2: Windows API (solo su Windows)
            if platform.system() == "Windows":
                xml_content, win_errors = self.extract_xml_from_p7m_windows(p7m_content)
                if xml_content:
                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(xml_content)
                    self.logger.debug(f"Decodifica Windows API riuscita per {input_file.name}")
                    return True, output_file, all_errors, "WINDOWS_API"
                all_errors.extend(win_errors)
            
            # Strategia 3: OpenSSL
            xml_content, openssl_errors = self.extract_xml_from_p7m_openssl(input_file)
            if xml_content:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(xml_content)
                self.logger.debug(f"Decodifica OpenSSL riuscita per {input_file.name}")
                return True, output_file, all_errors, "OPENSSL"
            all_errors.extend(openssl_errors)
            
        except Exception as e:
            all_errors.append(f"Errore lettura file: {str(e)}")
        
        self.stats['FAILED'] += 1
        self.logger.warning(f"Decodifica fallita per {input_file.name}")
        return False, None, all_errors, "FAILED"
    
    def get_statistics(self) -> Dict[str, int]:
        """Restituisce statistiche di decodifica."""
        return self.stats.copy()

# --- ANALISI XML AVANZATA ---
def parse_notification_xml(xml_content: str, filename: str) -> Optional[Dict]:
    """Parser per ricevute SDI e notifiche."""
    try:
        clean_xml = xml_content[xml_content.find('<?xml'):]
        it = ET.iterparse(io.StringIO(clean_xml))
        for _, el in it:
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]
        root = it.root
        
        def get_text(path): 
            elem = root.find(path)
            return elem.text.strip() if elem is not None and elem.text else None
        
        # Identifica tipo ricevuta
        if root.tag in ['RicevutaConsegna', 'NS']:
            tipo = "Ricevuta di Consegna"
        elif root.tag in ['NotificaEsito', 'NE']:
            tipo = "Notifica Esito"
        elif root.tag in ['NotificaMancataConsegna', 'MC']:
            tipo = "Notifica Mancata Consegna"
        elif root.tag in ['RicevutaScarto', 'NS']:
            tipo = "Ricevuta Scarto"
        elif root.tag in ['NotificaDecorrenzaTermini', 'DT']:
            tipo = "Notifica Decorrenza Termini"
        elif root.tag in ['AttestazioneTrasmissioneFattura', 'AT']:
            tipo = "Attestazione Trasmissione"
        else:
            tipo = f"Ricevuta Generica ({root.tag})"
        
        return {
            "status": "OK",
            "tipo_notifica": tipo,
            "identificativo_sdi": get_text(".//IdentificativoSdI"),
            "nome_file": get_text(".//NomeFile"),
            "hash_file": get_text(".//Hash"),
            "data_ora_ricezione": get_text(".//DataOraRicezione"),
            "data_ora_consegna": get_text(".//DataOraConsegna"),
            "riferimento_fattura": get_text(".//RiferimentoFattura"),
            "posizione_nella_fattura": get_text(".//PosizioneNellaFattura"),
            "raw_content": xml_content[:500]  # Prime 500 caratteristiche per debug
        }
        
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
            "tipo_notifica": "Errore parsing",
            "raw_content": xml_content[:200]
        }

def get_denominazione_or_nome_cognome(node) -> str:
    """Estrae denominazione o nome+cognome da nodo XML."""
    if node is None:
        return "Dati anagrafici mancanti"
    
    denominazione = node.find('Denominazione')
    if denominazione is not None and denominazione.text:
        return denominazione.text.strip()

    nome = node.find('Nome')
    cognome = node.find('Cognome')
    if nome is not None and cognome is not None:
        nome_text = nome.text.strip() if nome.text else ""
        cognome_text = cognome.text.strip() if cognome.text else ""
        if nome_text and cognome_text:
            return f"{nome_text} {cognome_text}"

    return "Dati anagrafici mancanti"

def parse_invoice_xml_advanced(xml_content: str) -> Optional[Dict]:
    """Parser XML fattura con estrazione completa dati."""
    try:
        clean_xml = xml_content[xml_content.find('<?xml'):]
        it = ET.iterparse(io.StringIO(clean_xml))
        for _, el in it:
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]
        root = it.root
        
        def get_text(path): 
            elem = root.find(path)
            return elem.text.strip() if elem is not None and elem.text else None
        
        def get_float(path):
            text = get_text(path)
            if text:
                try:
                    return float(text.replace(',', '.'))
                except:
                    return 0.0
            return 0.0
        
        # Estrae dati anagrafici
        header = root.find('FatturaElettronicaHeader')
        if not header:
            return None
            
        cedente = header.find('CedentePrestatore')
        cessionario = header.find('CessionarioCommittente')
        body = root.find('FatturaElettronicaBody')
        
        if not all([cedente, cessionario, body]):
            return None
            
        dati_generali = body.find('DatiGenerali')
        dati_generali_documento = dati_generali.find('DatiGeneraliDocumento')
        
        # Dati cedente
        cedente_anagrafica = cedente.find('DatiAnagrafici/Anagrafica')
        cedente_denominazione = get_denominazione_or_nome_cognome(cedente_anagrafica)
        cedente_id_fiscale = get_text(".//CedentePrestatore/DatiAnagrafici/CodiceFiscale") or "N/D"
        cedente_partita_iva = get_text(".//CedentePrestatore/DatiAnagrafici/IdFiscaleIVA/IdCodice") or "N/D"
        
        # Dati cessionario
        cessionario_anagrafica = cessionario.find('DatiAnagrafici/Anagrafica')
        cessionario_denominazione = get_denominazione_or_nome_cognome(cessionario_anagrafica)
        cessionario_id_fiscale = get_text(".//CessionarioCommittente/DatiAnagrafici/CodiceFiscale") or "N/D"
        cessionario_partita_iva = get_text(".//CessionarioCommittente/DatiAnagrafici/IdFiscaleIVA/IdCodice") or "N/D"
        
        # Dati generali fattura
        invoice_date_str = get_text(".//DatiGeneraliDocumento/Data")
        invoice_number = get_text(".//DatiGeneraliDocumento/Numero") or "N/D"
        total_amount = get_float(".//ImportoTotaleDocumento")
        
        # Parsing data
        invoice_date = None
        invoice_year = str(datetime.now().year)
        if invoice_date_str:
            try:
                invoice_date = datetime.strptime(invoice_date_str, '%Y-%m-%d').date()
                invoice_year = str(invoice_date.year)
            except:
                pass
        
        # Dati ritenute avanzati
        ritenuta_node = dati_generali_documento.find('DatiRitenuta')
        has_ritenuta = ritenuta_node is not None
        importo_ritenuta = 0.0
        tipo_ritenuta = "N/D"
        
        if has_ritenuta:
            importo_ritenuta = get_float(".//DatiRitenuta/ImportoRitenuta")
            tipo_ritenuta = get_text(".//DatiRitenuta/TipoRitenuta") or "N/D"
        
        # Dati cassa previdenziale
        cassa_node = dati_generali_documento.find('DatiCassaPrevidenziale')
        has_cassa = cassa_node is not None
        importo_cassa = 0.0
        if has_cassa:
            importo_cassa = get_float(".//DatiCassaPrevidenziale/ImportoContributoCassa")
        
        return {
            'invoice_number': invoice_number,
            'invoice_date': invoice_date,
            'invoice_year': invoice_year,
            'total_amount': total_amount,
            'cedente_denominazione': cedente_denominazione,
            'cedente_id_fiscale': cedente_id_fiscale,
            'cedente_partita_iva': cedente_partita_iva,
            'cessionario_denominazione': cessionario_denominazione,
            'cessionario_id_fiscale': cessionario_id_fiscale,
            'cessionario_partita_iva': cessionario_partita_iva,
            'has_ritenuta': has_ritenuta,
            'importo_ritenuta': importo_ritenuta,
            'tipo_ritenuta': tipo_ritenuta,
            'has_cassa': has_cassa,
            'importo_cassa': importo_cassa
        }
        
    except Exception as e:
        return None

# --- GESTIONE DUPLICATI AVANZATA ---
class AdvancedDuplicateManager:
    """Gestione avanzata duplicati con hash multipli e contenuto."""
    
    def __init__(self):
        self.processed_files = {}
        self.content_hashes = set()
        
    def is_duplicate(self, file_path: Path, xml_content: str) -> Tuple[bool, str]:
        """Verifica duplicati usando hash file e contenuto."""
        file_hash_md5 = calculate_file_hash(file_path, 'md5')
        file_hash_sha256 = calculate_file_hash(file_path, 'sha256')
        content_hash = calculate_content_hash(xml_content)
        
        # Crea chiave composta
        composite_key = f"{file_hash_sha256}_{content_hash}"
        
        # Verifica duplicati
        if composite_key in self.processed_files:
            return True, composite_key
        
        # Verifica anche solo contenuto normalizzato
        if content_hash in self.content_hashes:
            return True, f"content_{content_hash}"
        
        # Registra come processato
        self.processed_files[composite_key] = {
            'file_path': str(file_path),
            'file_hash_md5': file_hash_md5,
            'file_hash_sha256': file_hash_sha256,
            'content_hash': content_hash,
            'processed_at': datetime.now().isoformat()
        }
        self.content_hashes.add(content_hash)
        
        return False, composite_key
    
    def get_statistics(self) -> Dict[str, int]:
        """Restituisce statistiche duplicati."""
        return {
            'total_processed': len(self.processed_files),
            'unique_content': len(self.content_hashes)
        }

# --- LOGGING AVANZATO ---
def setup_logging(config: Dict) -> logging.Logger:
    log_config = config.get('logging', {})
    logger = logging.getLogger('AdeIntegratedAdvanced')
    logger.setLevel(getattr(logging, log_config.get('livello', 'INFO'), logging.INFO))
    logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if log_config.get('console_log', True):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if log_config.get('file_log', True):
        log_dir = Path(config['directory_sistema']['logs'])
        log_dir.mkdir(parents=True, exist_ok=True)

        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_dir / 'ade_integrated_advanced.log',
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# --- CONFIG MANAGER ---
class ConfigManager:
    def __init__(self, config_file: str = CONFIG_FILE):
        self.config_file = config_file
        self.config = self.load_config()

    def load_config(self) -> Dict:
        try:
            if not os.path.exists(self.config_file):
                self.create_default_config()

            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            self.validate_config(config)
            return config

        except Exception as e:
            print(f"ERRORE caricamento config: {e}")
            return self.get_minimal_config()

    def validate_config(self, config: Dict):
        required_sections = ['credenziali_ade', 'directory_sistema', 'portfolio_clienti']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Sezione mancante: {section}")

    def create_default_config(self):
        default_config = {
            "sistema": {
                "nome": "Sistema Integrato Fatture AdE - Advanced",
                "versione": "4.0",
                "preserva_case_nomi_file": True,
                "metadati_obbligatori": True,
                "decodifica_avanzata": True
            },
            "credenziali_ade": {
                "codice_fiscale": "",
                "pin": "",
                "password": "",
                "codice_fiscale_studio": ""
            },
            "portfolio_clienti": {
                "cliente_esempio": {
                    "nome_azienda": "Azienda Esempio SRL",
                    "partita_iva_diretta": "12345678901",
                    "codice_fiscale": "12345678901",
                    "profilo_accesso": 1,
                    "attivo": True
                }
            },
            "configurazione_download": {
                "tipi_documenti": {
                    "fatture_emesse": True,
                    "fatture_ricevute": True,
                    "fatture_passive": True,
                    "transfrontaliere_emesse": True,
                    "transfrontaliere_ricevute": True
                },
                "download_metadati": True,
                "download_ricevute_sdi": True,
                "decodifica_p7m": True,
                "pausa_tra_download": 0.5
            },
            "configurazione_decodifica": {
                "metodi_abilitati": {
                    "asn1": True,
                    "windows_api": True,
                    "openssl": True
                },
                "timeout_comando": 60,
                "tentativi_massimi": 3,
                "salva_errori_decodifica": True
            },
            "directory_sistema": {
                "input_temp": "temp_download_ade",
                "output_base": "aziende_processate",
                "archivio": "archivio_input",
                "logs": "logs_sistema",
                "reports": "reports_sistema"
            },
            "elaborazione": {
                "attiva": True,
                "cleanup_temp": True,
                "genera_report": True,
                "gestione_duplicati_avanzata": True
            },
            "logging": {
                "livello": "INFO",
                "file_log": True,
                "console_log": True
            }
        }

        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, ensure_ascii=False, indent=2)

        print(f"Configurazione creata: {self.config_file}")

    def get_minimal_config(self) -> Dict:
        return {
            "directory_sistema": {
                "input_temp": "temp_download_ade",
                "output_base": "aziende_processate",
                "archivio": "archivio_input",
                "logs": "logs_sistema",
                "reports": "reports_sistema"
            },
            "configurazione_decodifica": {
                "metodi_abilitati": {
                    "asn1": True,
                    "windows_api": True,
                    "openssl": True
                },
                "timeout_comando": 60,
                "tentativi_massimi": 3,
                "salva_errori_decodifica": True
            },
            "logging": {"livello": "INFO", "file_log": True, "console_log": True},
            "sistema": {"preserva_case_nomi_file": True, "metadati_obbligatori": True, "decodifica_avanzata": True}
        }

    def get_active_clients(self) -> Dict[str, Dict]:
        clients = self.config.get('portfolio_clienti', {})
        return {k: v for k, v in clients.items() if v.get('attivo', True)}

# --- ORGANIZZATORE FILE AVANZATO ---
class AdvancedFileOrganizer:
    def __init__(self, config: Dict, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.decoder = AdvancedP7MDecoder(logger)
        self.duplicate_manager = AdvancedDuplicateManager()
        self.output_base = Path(config['directory_sistema']['output_base'])
        self.clients_config = config.get('portfolio_clienti', {})
    
    def get_client_name(self, client_id: str) -> str:
        client_data = self.clients_config.get(client_id, {})
        return client_data.get('nome_azienda', client_id)
    
    def normalize_file_type(self, file_type: str) -> str:
        type_mapping = {
            'emesse': 'EMESSE',
            'ricevute_ricezione': 'RICEVUTE', 
            'ricevute_emissione': 'RICEVUTE',
            'ricevute': 'RICEVUTE',
            'passive': 'RICEVUTE',
            'transfrontaliere_emesse': 'TRANSFRONTALIERE_EMESSE',
            'transfrontaliere_ricevute': 'TRANSFRONTALIERE_RICEVUTE'
        }
        return type_mapping.get(file_type, file_type.upper())
    
    def create_client_structure(self, client_name: str, file_type: str, anno: int) -> Dict[str, Path]:
        normalized_type = self.normalize_file_type(file_type)
        base_path = self.output_base / client_name / normalized_type / str(anno)
        
        structure = {
            'base': base_path,
            'json': base_path / 'json',
            'xml_decodificati': base_path / 'xml_decodificati',
            'p7m_originali': base_path / 'p7m_originali',
            'ricevute_sdi': base_path / 'ricevute_sdi',
            'metadati': base_path / 'metadati'
        }
        
        for dir_path in structure.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        return structure
    
    def process_single_file_advanced(self, file_path: Path, is_temp: bool = False) -> Tuple[AdvancedProcessingResult, Optional[Dict]]:
        """Processa singolo file con sistema avanzato."""
        result = AdvancedProcessingResult(
            file_name=file_path.name, 
            status="KO", 
            method_used="NONE",
            original_size=file_path.stat().st_size if file_path.exists() else 0
        )
        
        try:
            # Determina tipo file
            file_type = determine_file_type(file_path)
            result.file_type = file_type
            
            if file_type == "UNSUPPORTED":
                result.status = "SKIPPED"
                result.error_message = "Tipo file non supportato"
                return result, None
            
            # Calcola hash
            result.hash_md5 = calculate_file_hash(file_path, 'md5')
            result.hash_sha256 = calculate_file_hash(file_path, 'sha256')
            
            # Estrae XML
            xml_content = None
            decoding_method = "NONE"
            
            if file_path.suffix.lower() == '.p7m':
                # Usa decodifica avanzata
                temp_dir = file_path.parent / 'temp_decode'
                success, decoded_file, errors, method = self.decoder.decrypt_p7m_file(file_path, temp_dir)
                
                result.decoding_attempts = [method] if method != "FAILED" else ["ASN1_FAILED", "WINDOWS_API_FAILED", "OPENSSL_FAILED"]
                
                if success and decoded_file and decoded_file.exists():
                    xml_content = decoded_file.read_text(encoding='utf-8', errors='ignore')
                    decoding_method = method
                    result.decoded_size = decoded_file.stat().st_size
                    
                    # Cleanup temp
                    try:
                        shutil.rmtree(temp_dir)
                    except:
                        pass
                else:
                    result.error_message = f"Decodifica P7M fallita: {'; '.join(errors[:3])}"
                    return result, None
            else:
                xml_content = file_path.read_text(encoding='utf-8', errors='ignore')
                decoding_method = "DIRECT_XML"
                result.decoded_size = result.original_size
            
            result.method_used = decoding_method
            
            # Controlla duplicati
            if self.config.get('elaborazione', {}).get('gestione_duplicati_avanzata', True):
                is_dup, dup_key = self.duplicate_manager.is_duplicate(file_path, xml_content)
                if is_dup:
                    result.status = "SKIPPED"
                    result.is_duplicate = True
                    result.error_message = f"Duplicato: {dup_key[:32]}..."
                    return result, None
            
            # Parsing specifico per tipo
            parsed_data = None
            
            if file_type == "NOTIFICATION":
                notification_data = parse_notification_xml(xml_content, file_path.name)
                if notification_data and notification_data.get("status") == "OK":
                    parsed_data = notification_data
                    result.company_name = "RICEVUTE_SDI"
                else:
                    result.error_message = "Parsing ricevuta fallito"
                    return result, None
                    
            elif file_type == "METADATA":
                # Metadati - salva direttamente
                result.company_name = "METADATI"
                result.status = "OK"
                parsed_data = {"type": "metadata", "content": xml_content}
                
            elif file_type == "INVOICE":
                invoice_data = parse_invoice_xml_advanced(xml_content)
                if invoice_data:
                    parsed_data = invoice_data
                    result.company_name = "FATTURA"  # Sarà determinato dal parser portfolio
                    result.invoice_year = invoice_data.get('invoice_year')
                    result.has_ritenuta = invoice_data.get('has_ritenuta', False)
                    result.importo_ritenuta = invoice_data.get('importo_ritenuta', 0.0)
                else:
                    result.error_message = "Parsing fattura fallito o non del portfolio"
                    return result, None
            
            result.status = "OK"
            return result, parsed_data
            
        except Exception as e:
            result.error_message = f"Errore elaborazione: {str(e)}"
            self.logger.error(f"Errore elaborazione {file_path.name}: {e}")
            return result, None
    
    def save_organized_file_advanced(self, file_path: Path, xml_content: str, parsed_data: Dict, 
                                   result: AdvancedProcessingResult, client_name: str = None) -> bool:
        """Salva file organizzato con metadati avanzati."""
        try:
            if result.file_type == "INVOICE":
                # Determina cliente dal portfolio
                # Qui dovresti implementare la logica per determinare il cliente
                # basandoti sui dati della fattura e il portfolio configurato
                if not client_name:
                    client_name = "Cliente_Sconosciuto"
                
                year = parsed_data.get('invoice_year', str(datetime.now().year))
                direction = "RICEVUTE"  # Default, dovrebbe essere determinato dal parsing
                
                structure = self.create_client_structure(client_name, direction, int(year))
                
            elif result.file_type == "NOTIFICATION":
                # Ricevute SDI
                client_name = client_name or "Ricevute_SDI"
                year = str(datetime.now().year)
                structure = self.create_client_structure(client_name, "RICEVUTE", int(year))
                
            elif result.file_type == "METADATA":
                # Metadati
                client_name = client_name or "Metadati_Generici"
                year = str(datetime.now().year)
                structure = self.create_client_structure(client_name, "METADATI", int(year))
            
            base_name = safe_filename(file_path.stem)
            
            # Genera metadati completi
            metadata = {
                "id": str(uuid.uuid4()),
                "fileName": file_path.name,
                "fileType": result.file_type,
                "company": {"name": client_name},
                "processing": {
                    "method_used": result.method_used,
                    "decoding_attempts": result.decoding_attempts,
                    "file_hash_md5": result.hash_md5,
                    "file_hash_sha256": result.hash_sha256,
                    "original_size": result.original_size,
                    "decoded_size": result.decoded_size,
                    "processed_at": datetime.now(timezone.utc).isoformat()
                },
                "parsedData": parsed_data
            }
            
            # Salva JSON
            json_path = structure['json'] / f"{base_name}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2, default=str)
            
            # Salva XML decodificato
            xml_path = structure['xml_decodificati'] / f"{base_name}.xml"
            with open(xml_path, 'w', encoding='utf-8') as f:
                f.write(xml_content)
            
            # Salva originale se P7M
            if file_path.suffix.lower() == '.p7m':
                orig_path = structure['p7m_originali'] / file_path.name
                shutil.copy2(file_path, orig_path)
            
            self.logger.info(f"[OK] Organizzato: {client_name}/{year}/{result.file_type}/{base_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Errore salvataggio {file_path.name}: {e}")
            return False
    
    def organize_downloaded_files(self, download_results: Dict[str, List[DownloadResult]], 
                                 decode: bool = True) -> OrganizationResult:
        result = OrganizationResult(success=True)
        
        try:
            total_files = sum(len(results) for results in download_results.values())
            
            with tqdm(total=total_files, desc="Organizzazione Avanzata", unit="file") as pbar:
                for client_id, results in download_results.items():
                    client_name = self.get_client_name(client_id)
                    client_folders = []
                    
                    for download_result in results:
                        if download_result.success and download_result.file_path:
                            # Processa con sistema avanzato
                            proc_result, parsed_data = self.process_single_file_advanced(download_result.file_path)
                            
                            if proc_result.status == "OK" and parsed_data:
                                success = self.save_organized_file_advanced(
                                    download_result.file_path, 
                                    "", # XML content dovrebbe essere estratto qui
                                    parsed_data,
                                    proc_result,
                                    client_name
                                )
                                
                                if success:
                                    result.organized_files += 1
                                    # Aggiorna statistiche decodifica
                                    if proc_result.method_used != "NONE":
                                        result.decoding_stats[proc_result.method_used] = result.decoding_stats.get(proc_result.method_used, 0) + 1
                                else:
                                    result.errors.append(f"Errore salvataggio {download_result.file_path.name}")
                            else:
                                result.errors.append(f"Errore elaborazione {download_result.file_path.name}: {proc_result.error_message}")
                        
                        pbar.update(1)
                    
                    result.client_folders_created[client_id] = client_folders
            
            return result
            
        except Exception as e:
            result.success = False
            result.errors.append(str(e))
            return result

    def get_processing_statistics(self) -> Dict:
        """Restituisce statistiche complete di elaborazione."""
        decoder_stats = self.decoder.get_statistics()
        duplicate_stats = self.duplicate_manager.get_statistics()
        
        return {
            "decoder": decoder_stats,
            "duplicates": duplicate_stats,
            "total_methods": sum(decoder_stats.values())
        }

# --- DOWNLOADER ADE (INVARIATO) ---
class CompleteAdeDownloader:
    def __init__(self, config: Dict, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = None
        self.headers_token = {}
        self.p_auth = ""
        self.temp_dir = Path(config['directory_sistema']['input_temp'])
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.download_config = config.get('configurazione_download', {})

    def create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            'User-Agent': DEFAULT_USER_AGENT,
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7'
        })
        session.cookies.set('LFR_SESSION_STATE_20159', 'expired', domain='ivaservizi.agenziaentrate.gov.it')
        session.cookies.set('LFR_SESSION_STATE_10811916', unix_timestamp(), domain='ivaservizi.agenziaentrate.gov.it')
        return session

    def login(self, cf: str, pin: str, password: str) -> bool:
        try:
            self.session = self.create_session()

            self.logger.info("Collegamento portale AdE...")
            r = self.session.get('https://ivaservizi.agenziaentrate.gov.it/portale/web/guest', verify=False, timeout=30)
            if r.status_code != 200:
                raise Exception(f"Collegamento fallito: {r.status_code}")

            self.logger.info("Autenticazione...")
            payload = {
                '_58_saveLastPath': 'false',
                '_58_redirect': '',
                '_58_doActionAfterLogin': 'false',
                '_58_login': cf,
                '_58_pin': pin,
                '_58_password': password
            }

            r = self.session.post(
                'https://ivaservizi.agenziaentrate.gov.it/portale/home?p_p_id=58&p_p_lifecycle=1&p_p_state=normal&p_p_mode=view&p_p_col_id=column-1&p_p_col_pos=3&p_p_col_count=4&_58_struts_action=%2Flogin%2Flogin',
                data=payload, verify=False, timeout=30
            )

            liferay_matches = re.findall(r"Liferay\.authToken\s*=\s*'([^']+)';", r.text)
            if not liferay_matches:
                raise Exception("Token non trovato")

            self.p_auth = liferay_matches[0]

            r = self.session.get(f'https://ivaservizi.agenziaentrate.gov.it/dp/api?v={unix_timestamp()}', timeout=30)
            if r.status_code != 200:
                raise Exception(f"Verifica login fallita: {r.status_code}")

            self.logger.info("✅ Autenticazione completata")
            return True

        except Exception as e:
            self.logger.error(f"❌ Errore login: {e}")
            return False

    def select_client_profile(self, client_data: Dict) -> bool:
        try:
            profilo = client_data.get('profilo_accesso', 1)
            cf_cliente = client_data.get('codice_fiscale', '')
            piva_diretta = client_data.get('partita_iva_diretta', cf_cliente)

            self.logger.info(f"Selezione profilo {profilo}")

            base_url = 'https://ivaservizi.agenziaentrate.gov.it/portale/scelta-utenza-lavoro'
            base_params = f'p_auth={self.p_auth}&p_p_id=SceltaUtenzaLavoro_WAR_SceltaUtenzaLavoroportlet&p_p_lifecycle=1&p_p_state=normal&p_p_mode=view&p_p_col_id=column-1&p_p_col_count=1'

            if profilo == 1:
                payload = {'cf_inserito': cf_cliente}
                r = self.session.post(
                    f'{base_url}?{base_params}&_SceltaUtenzaLavoro_WAR_SceltaUtenzaLavoroportlet_javax.portlet.action=delegaDirettaAction',
                    data=payload, timeout=30
                )
                payload.update({'sceltapiva': piva_diretta})
                r = self.session.post(
                    f'{base_url}?{base_params}&_SceltaUtenzaLavoro_WAR_SceltaUtenzaLavoroportlet_javax.portlet.action=delegaDirettaAction',
                    data=payload, timeout=30
                )

            return r.status_code == 200

        except Exception as e:
            self.logger.error(f"Errore selezione profilo: {e}")
            return False

    def setup_service_headers(self) -> bool:
        try:
            r = self.session.get('https://ivaservizi.agenziaentrate.gov.it/ser/api/fatture/v1/ul/me/adesione/stato/', timeout=30)
            if r.status_code != 200:
                raise Exception("Adesione servizio fallita")

            r = self.session.get(
                f'https://ivaservizi.agenziaentrate.gov.it/cons/cons-services/sc/tokenB2BCookie/get?v={unix_timestamp()}',
                timeout=30
            )

            xb2bcookie = r.headers.get('x-b2bcookie')
            xtoken = r.headers.get('x-token')

            if not xb2bcookie or not xtoken:
                raise Exception("Token B2B mancanti")

            self.session.headers.update({
                'x-b2bcookie': xb2bcookie,
                'x-token': xtoken
            })

            self.headers_token = self.session.headers.copy()
            self.logger.info("✅ Headers configurati")
            return True

        except Exception as e:
            self.logger.error(f"❌ Errore setup headers: {e}")
            return False

    def download_invoice_file(self, fattura_file: str, file_type: str, output_dir: Path) -> Optional[Path]:
        try:
            file_type_param = "FILE_FATTURA" if file_type == "fattura" else "FILE_METADATI"
            url = f'https://ivaservizi.agenziaentrate.gov.it/cons/cons-services/rs/fatture/file/{fattura_file}?tipoFile={file_type_param}&download=1&v={unix_timestamp()}'

            r = self.session.get(url, headers=self.headers_token, stream=True, timeout=60)

            if r.status_code != 200:
                return None

            content_disp = r.headers.get('content-disposition', '')
            fname = _parse_filename_from_content_disposition(content_disp)

            if not fname:
                return None

            file_path = output_dir / fname

            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return file_path

        except Exception as e:
            self.logger.error(f"Errore download {fattura_file}: {e}")
            return None

    def download_invoices_by_type(self, tipo_fatture: str, data_inizio: str, data_fine: str, 
                                 client_id: str, client_data: Dict) -> List[DownloadResult]:
        results = []

        try:
            urls = {
                'emesse': f'https://ivaservizi.agenziaentrate.gov.it/cons/cons-services/rs/fe/emesse/dal/{data_inizio}/al/{data_fine}?v={unix_timestamp()}',
                'ricevute_ricezione': f'https://ivaservizi.agenziaentrate.gov.it/cons/cons-services/rs/fe/ricevute/dal/{data_inizio}/al/{data_fine}/ricerca/ricezione?v={unix_timestamp()}',
                'passive': f'https://ivaservizi.agenziaentrate.gov.it/cons/cons-services/rs/fe/mc/dal/{data_inizio}/al/{data_fine}?v={unix_timestamp()}'
            }

            if tipo_fatture not in urls:
                return results

            self.logger.info(f"📥 Download {tipo_fatture}")
            r = self.session.get(urls[tipo_fatture], headers=self.headers_token, timeout=60)

            if r.status_code != 200:
                return [DownloadResult(success=False, error_message=f"Lista non disponibile: {r.status_code}", file_type=tipo_fatture, client_id=client_id)]

            data = r.json()
            fatture = data.get('fatture', [])

            if not fatture:
                self.logger.info(f"Nessuna fattura {tipo_fatture}")
                return results

            self.logger.info(f"Trovate {len(fatture)} fatture {tipo_fatture}")

            type_dir = self.temp_dir / f"{tipo_fatture}_{client_id}"
            type_dir.mkdir(parents=True, exist_ok=True)

            with tqdm(fatture, desc=f"Download {tipo_fatture}") as pbar:
                for fattura in pbar:
                    try:
                        fattura_file = fattura['tipoInvio'] + fattura['idFattura']
                        result = DownloadResult(success=True, file_type=tipo_fatture, client_id=client_id)

                        main_file = self.download_invoice_file(fattura_file, "fattura", type_dir)
                        if main_file:
                            result.file_path = main_file
                            meta_file = self.download_invoice_file(fattura_file, "metadati", type_dir)
                            if meta_file:
                                result.metadata_path = meta_file
                        else:
                            result.success = False
                            result.error_message = "Download fallito"

                        results.append(result)

                    except Exception as e:
                        results.append(DownloadResult(
                            success=False, error_message=str(e), file_type=tipo_fatture, client_id=client_id
                        ))

            return results

        except Exception as e:
            return [DownloadResult(success=False, error_message=str(e), file_type=tipo_fatture, client_id=client_id)]

    def download_client_invoices(self, client_id: str, client_data: Dict, 
                                data_inizio: str, data_fine: str) -> List[DownloadResult]:
        all_results = []

        try:
            if not self.select_client_profile(client_data):
                return [DownloadResult(success=False, error_message="Selezione profilo fallita", client_id=client_id)]

            if not self.setup_service_headers():
                return [DownloadResult(success=False, error_message="Setup headers fallito", client_id=client_id)]

            tipi_docs = self.download_config.get('tipi_documenti', {})

            if tipi_docs.get('fatture_emesse', True):
                results = self.download_invoices_by_type('emesse', data_inizio, data_fine, client_id, client_data)
                all_results.extend(results)

            if tipi_docs.get('fatture_ricevute', True):
                results = self.download_invoices_by_type('ricevute_ricezione', data_inizio, data_fine, client_id, client_data)
                all_results.extend(results)

            if tipi_docs.get('fatture_passive', True):
                results = self.download_invoices_by_type('passive', data_inizio, data_fine, client_id, client_data)
                all_results.extend(results)

            return all_results

        except Exception as e:
            return [DownloadResult(success=False, error_message=str(e), client_id=client_id)]

# --- SISTEMA INTEGRATO AVANZATO ---
class AdvancedIntegratedSystem:
    def __init__(self, config_file: str = CONFIG_FILE):
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.config
        self.logger = setup_logging(self.config)
        self.downloader = CompleteAdeDownloader(self.config, self.logger)
        self.organizer = AdvancedFileOrganizer(self.config, self.logger)

        self.setup_directories()
        self.temp_dir = Path(self.config['directory_sistema']['input_temp'])
        self.logger.info(f"✅ Sistema Avanzato inizializzato - {SCRIPT_VERSION}")

    def setup_directories(self):
        dirs = self.config['directory_sistema']
        for _, dir_path in dirs.items():
            Path(dir_path).mkdir(parents=True, exist_ok=True)

    def download_only(self, data_inizio: str, data_fine: str) -> Dict[str, List[DownloadResult]]:
        self.logger.info("🚀 MODALITÀ SOLO SCARICAMENTO AVANZATO")
        return self.download_period(data_inizio, data_fine)

    def decode_only_advanced(self, source_dir: Optional[Path] = None) -> Dict:
        """Decodifica avanzata con statistiche dettagliate."""
        self.logger.info("🔓 MODALITÀ DECODIFICA AVANZATA")
        
        if source_dir is None:
            source_dir = self.temp_dir
        
        if not source_dir.exists():
            return {'success': False, 'error': f'Directory non trovata: {source_dir}'}

        try:
            decoder = AdvancedP7MDecoder(self.logger)
            total_decoded = 0
            errors = []
            method_stats = {}

            p7m_files = list(source_dir.glob("**/*.p7m"))
            
            if not p7m_files:
                return {
                    'success': True, 
                    'decoded_files': 0, 
                    'errors': [],
                    'method_statistics': {}
                }

            with tqdm(p7m_files, desc="Decodifica P7M Avanzata") as pbar:
                for p7m_file in pbar:
                    try:
                        output_dir = p7m_file.parent / "decoded_advanced"
                        success, decoded_file, decode_errors, method = decoder.decrypt_p7m_file(p7m_file, output_dir)
                        
                        if success:
                            total_decoded += 1
                            method_stats[method] = method_stats.get(method, 0) + 1
                            pbar.set_postfix({"Metodo": method, "Successi": total_decoded})
                        else:
                            errors.extend(decode_errors[:2])  # Limita errori per file
                            
                    except Exception as e:
                        errors.append(f"Errore {p7m_file.name}: {str(e)}")

            # Ottieni statistiche complete dal decoder
            decoder_stats = decoder.get_statistics()

            return {
                'success': len(errors) < total_decoded * 0.1,  # Successo se < 10% errori
                'decoded_files': total_decoded,
                'errors': errors,
                'method_statistics': decoder_stats,
                'file_method_distribution': method_stats
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def organize_only_advanced(self, source_dir: Optional[Path] = None, decode: bool = True) -> OrganizationResult:
        """Organizzazione avanzata con elaborazione intelligente."""
        self.logger.info("📁 MODALITÀ RIORGANIZZAZIONE AVANZATA")
        
        if source_dir is None:
            source_dir = self.temp_dir
        
        try:
            if not source_dir.exists():
                result = OrganizationResult(success=False)
                result.errors.append(f"Directory non trovata: {source_dir}")
                return result
            
            # Trova tutti i file supportati
            all_files = []
            for ext in SUPPORTED_EXTENSIONS:
                all_files.extend(source_dir.glob(f"**/*{ext}"))
            
            result = OrganizationResult(success=True)
            
            with tqdm(all_files, desc="Riorganizzazione Avanzata", unit="file") as pbar:
                for file_path in pbar:
                    try:
                        # Processa con sistema avanzato
                        proc_result, parsed_data = self.organizer.process_single_file_advanced(file_path)
                        
                        if proc_result.status == "OK" and parsed_data:
                            # Estrae XML per salvataggio
                            xml_content = ""
                            if file_path.suffix.lower() == '.xml':
                                xml_content = file_path.read_text(encoding='utf-8', errors='ignore')
                            elif file_path.suffix.lower() == '.p7m':
                                # Decodifica per ottenere XML
                                temp_dir = file_path.parent / 'temp_decode_org'
                                success, decoded_file, _, _ = self.organizer.decoder.decrypt_p7m_file(file_path, temp_dir)
                                if success and decoded_file and decoded_file.exists():
                                    xml_content = decoded_file.read_text(encoding='utf-8', errors='ignore')
                                    # Cleanup
                                    try:
                                        shutil.rmtree(temp_dir)
                                    except:
                                        pass
                            
                            # Salva organizzato
                            success = self.organizer.save_organized_file_advanced(
                                file_path, xml_content, parsed_data, proc_result, "Cliente_Riorganizzato"
                            )
                            
                            if success:
                                result.organized_files += 1
                                # Aggiorna statistiche
                                if proc_result.method_used != "NONE":
                                    result.decoding_stats[proc_result.method_used] = result.decoding_stats.get(proc_result.method_used, 0) + 1
                            else:
                                result.errors.append(f"Errore salvataggio {file_path.name}")
                        else:
                            if proc_result.status == "SKIPPED":
                                continue  # Non è un errore
                            result.errors.append(f"Errore elaborazione {file_path.name}: {proc_result.error_message}")
                        
                        pbar.set_postfix({
                            "Elaborati": result.organized_files,
                            "Errori": len(result.errors)
                        })
                        
                    except Exception as e:
                        result.errors.append(f"Errore {file_path.name}: {str(e)}")
            
            # Aggiungi statistiche complete
            processing_stats = self.organizer.get_processing_statistics()
            result.decoding_stats.update(processing_stats.get('decoder', {}))
            
            return result
            
        except Exception as e:
            result = OrganizationResult(success=False)
            result.errors.append(str(e))
            return result

    def download_period(self, data_inizio: str, data_fine: str) -> Dict[str, List[DownloadResult]]:
        all_results = {}

        try:
            creds = self.config['credenziali_ade']
            if not self.downloader.login(creds['codice_fiscale'], creds['pin'], creds['password']):
                raise Exception("Autenticazione fallita")

            trimestri = divide_in_trimestri(data_inizio, data_fine)
            active_clients = self.config_manager.get_active_clients()

            for trimestre in trimestri:
                inizio, fine = trimestre
                self.logger.info(f"📅 Trimestre: {inizio} - {fine}")

                for client_id, client_data in active_clients.items():
                    results = self.downloader.download_client_invoices(client_id, client_data, inizio, fine)

                    if client_id not in all_results:
                        all_results[client_id] = []
                    all_results[client_id].extend(results)

            return all_results

        except Exception as e:
            self.logger.error(f"Errore download: {e}")
            return {}

    def full_workflow_advanced(self, data_inizio: str, data_fine: str, decode: bool = True) -> Dict:
        """Workflow completo con sistema avanzato."""
        try:
            self.logger.info("🚀 WORKFLOW COMPLETO AVANZATO")

            # Download
            download_results = self.download_period(data_inizio, data_fine)
            if not download_results:
                return {'success': False, 'error': 'Nessun file scaricato'}

            # Decodifica avanzata
            decode_results = {'success': True, 'decoded_files': 0}
            if decode:
                decode_results = self.decode_only_advanced()

            # Organizzazione avanzata
            org_results = self.organizer.organize_downloaded_files(download_results, decode)

            # Statistiche complete
            processing_stats = self.organizer.get_processing_statistics()

            return {
                'success': True,
                'download_results': download_results,
                'decode_results': decode_results,
                'organization_results': org_results,
                'processing_statistics': processing_stats
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

# --- REPORT AVANZATO ---
def generate_advanced_report(results: List[AdvancedProcessingResult], 
                           processing_stats: Dict) -> str:
    """Genera report avanzato con statistiche dettagliate."""
    total = len(results)
    ok_count = sum(1 for r in results if r.status == "OK")
    ko_count = sum(1 for r in results if r.status == "KO")
    skip_count = sum(1 for r in results if r.status == "SKIPPED")
    
    # Statistiche per tipo file
    type_stats = {}
    for r in results:
        if r.file_type:
            type_stats[r.file_type] = type_stats.get(r.file_type, 0) + 1
    
    # Statistiche decodifica
    method_stats = {}
    for r in results:
        if r.method_used != "NONE":
            method_stats[r.method_used] = method_stats.get(r.method_used, 0) + 1
    
    # Statistiche ritenute
    fatture_con_ritenuta = sum(1 for r in results if r.has_ritenuta)
    totale_ritenute = sum(r.importo_ritenuta for r in results if r.has_ritenuta)
    
    # Statistiche dimensioni
    total_original_size = sum(r.original_size for r in results if r.original_size > 0)
    total_decoded_size = sum(r.decoded_size for r in results if r.decoded_size > 0)
    
    companies = set(r.company_name for r in results if r.company_name)
    years = set(r.invoice_year for r in results if r.invoice_year)
    
    report = [
        f"\n{'='*80}",
        f"REPORT ELABORAZIONE AVANZATA - {SCRIPT_VERSION}",
        f"{'='*80}",
        f"\n📊 STATISTICHE GENERALI:",
        f"  • File totali: {total}",
        f"  • Elaborati OK: {ok_count}",
        f"  • Errori: {ko_count}",
        f"  • Saltati: {skip_count}",
        f"  • Tasso successo: {(ok_count/total*100):.1f}%" if total > 0 else "",
        f"\n📁 STATISTICHE TIPI FILE:"
    ]
    
    for file_type, count in type_stats.items():
        report.append(f"  • {file_type}: {count}")
    
    report.extend([
        f"\n🔓 METODI DECODIFICA:",
    ])
    
    for method, count in method_stats.items():
        report.append(f"  • {method}: {count}")
    
    if processing_stats:
        decoder_stats = processing_stats.get('decoder', {})
        if decoder_stats:
            report.extend([
                f"\n📈 STATISTICHE DECODER:",
                f"  • ASN1 successi: {decoder_stats.get('ASN1_SUCCESS', 0)}",
                f"  • Windows API successi: {decoder_stats.get('WINDOWS_API_SUCCESS', 0)}",
                f"  • OpenSSL successi: {decoder_stats.get('OPENSSL_SUCCESS', 0)}",
                f"  • Fallimenti: {decoder_stats.get('FAILED', 0)}"
            ])
    
    report.extend([
        f"\n💰 RITENUTE:",
        f"  • Fatture con ritenuta: {fatture_con_ritenuta}",
        f"  • Totale ritenute: EUR{totale_ritenute:.2f}",
        f"  • Media ritenuta: EUR{(totale_ritenute/fatture_con_ritenuta):.2f}" if fatture_con_ritenuta > 0 else "",
        f"\n📏 DIMENSIONI:",
        f"  • Dimensione totale originale: {total_original_size/1024/1024:.1f} MB",
        f"  • Dimensione totale decodificata: {total_decoded_size/1024/1024:.1f} MB",
        f"  • Rapporto compressione: {(total_decoded_size/total_original_size*100):.1f}%" if total_original_size > 0 else ""
    ])
    
    if companies:
        report.append(f"\nAZIENDE: {', '.join(companies)}")
    if years:
        report.append(f"ANNI: {', '.join(sorted(years))}")
    
    return "\n".join(report)

# --- CLI AVANZATA ---
def print_banner():
    banner = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              SISTEMA INTEGRATO FATTURE ELETTRONICHE - ADVANCED               ║
║  Versione: {SCRIPT_VERSION:58} ║
║                            Decodifica Multi-Strategia                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
    print(banner)

def create_argument_parser():
    parser = argparse.ArgumentParser(description='Sistema Fatture AdE Advanced v4.0')
    subparsers = parser.add_subparsers(dest='command', help='Comandi disponibili')

    # Comando manuale avanzato
    manual_parser = subparsers.add_parser('manual', help='Esecuzione manuale avanzata')
    manual_parser.add_argument('data_inizio', help='Data inizio (DD/MM/YYYY)')
    manual_parser.add_argument('data_fine', help='Data fine (DD/MM/YYYY)')
    manual_parser.add_argument('--SCA', action='store_true', help='Solo scaricamento')
    manual_parser.add_argument('--DEC', action='store_true', help='Include decodifica avanzata')
    manual_parser.add_argument('--ORG', action='store_true', help='Include organizzazione avanzata')
    manual_parser.add_argument('--no-decode', action='store_true', help='Non decodificare')
    manual_parser.add_argument('--stats', action='store_true', help='Mostra statistiche dettagliate')

    # Comando decodifica avanzata
    decode_parser = subparsers.add_parser('decode-advanced', help='Decodifica avanzata')
    decode_parser.add_argument('--DEC', action='store_true', required=True)
    decode_parser.add_argument('--source', type=str, help='Directory sorgente')
    decode_parser.add_argument('--stats', action='store_true', help='Mostra statistiche decoder')

    # Comando organizzazione avanzata
    organize_parser = subparsers.add_parser('organize-advanced', help='Organizzazione avanzata')
    organize_parser.add_argument('--ORG', action='store_true', required=True)
    organize_parser.add_argument('--source', type=str, help='Directory sorgente')
    organize_parser.add_argument('--no-decode', action='store_true', help='Non decodificare')
    organize_parser.add_argument('--stats', action='store_true', help='Mostra statistiche elaborazione')

    # Altri comandi
    subparsers.add_parser('config', help='Mostra configurazione')
    subparsers.add_parser('template', help='Template configurazione')
    subparsers.add_parser('test-login', help='Test credenziali')
    subparsers.add_parser('test-decoder', help='Test decoder P7M')

    return parser

def validate_date_format(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%d/%m/%Y")
        return True
    except ValueError:
        return False

def show_config_template():
    print("\n📋 TEMPLATE CONFIGURAZIONE AVANZATA:")
    template = {
        "credenziali_ade": {
            "codice_fiscale": "ESEMPIO",
            "pin": "12345678",
            "password": "password"
        },
        "portfolio_clienti": {
            "cliente1": {
                "nome_azienda": "Azienda Test SRL",
                "partita_iva_diretta": "12345678901",
                "codice_fiscale": "12345678901",
                "profilo_accesso": 1,
                "attivo": True
            }
        },
        "configurazione_decodifica": {
            "metodi_abilitati": {
                "asn1": True,
                "windows_api": True,
                "openssl": True
            },
            "timeout_comando": 60,
            "tentativi_massimi": 3
        }
    }
    print(json.dumps(template, ensure_ascii=False, indent=2))

def test_decoder_functionality():
    """Testa le funzionalità del decoder."""
    print("\n🔧 TEST DECODER P7M:")
    
    print(f"ASN1Crypto disponibile: {'✅' if ASN1_AVAILABLE else '❌'}")
    print(f"Windows API disponibile: {'✅' if WIN32_AVAILABLE else '❌'}")
    
    # Test OpenSSL
    try:
        result = subprocess.run(['openssl', 'version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print(f"OpenSSL disponibile: ✅ ({result.stdout.strip()})")
        else:
            print("OpenSSL disponibile: ❌")
    except:
        print("OpenSSL disponibile: ❌")

def main():
    try:
        print_banner()
        
        parser = create_argument_parser()
        
        if len(sys.argv) < 2:
            parser.print_help()
            return
        
        args = parser.parse_args()

        try:
            system = AdvancedIntegratedSystem()
        except Exception as e:
            print(f"❌ ERRORE inizializzazione: {e}")
            return

        if args.command == 'config':
            print("📋 CONFIGURAZIONE AVANZATA:")
            config_copy = json.loads(json.dumps(system.config))
            if 'credenziali_ade' in config_copy:
                for key in config_copy['credenziali_ade']:
                    if config_copy['credenziali_ade'][key]:
                        config_copy['credenziali_ade'][key] = '***'
            print(json.dumps(config_copy, ensure_ascii=False, indent=2))

        elif args.command == 'template':
            show_config_template()

        elif args.command == 'test-decoder':
            test_decoder_functionality()

        elif args.command == 'test-login':
            print("🔐 TEST LOGIN")
            creds = system.config['credenziali_ade']
            if not all([creds.get('codice_fiscale'), creds.get('pin'), creds.get('password')]):
                print("❌ Credenziali mancanti")
                return

            success = system.downloader.login(creds['codice_fiscale'], creds['pin'], creds['password'])
            if success:
                print("✅ Login completato")
            else:
                print("❌ Login fallito")

        elif args.command == 'manual':
            if not validate_date_format(args.data_inizio) or not validate_date_format(args.data_fine):
                print("❌ Formato date non valido")
                return

            data_inizio = datetime.strptime(args.data_inizio, "%d/%m/%Y").strftime("%d%m%Y")
            data_fine = datetime.strptime(args.data_fine, "%d/%m/%Y").strftime("%d%m%Y")

            print(f"🚀 ELABORAZIONE AVANZATA: {args.data_inizio} → {args.data_fine}")

            # Determina operazioni
            do_download = args.SCA or (not args.SCA and not args.DEC and not args.ORG)
            do_decode = args.DEC or (not args.SCA and not args.DEC and not args.ORG)    
            do_organize = args.ORG or (not args.SCA and not args.DEC and not args.ORG)
            
            if args.no_decode:
                do_decode = False

            operations = []
            if do_download: operations.append("SCARICAMENTO")
            if do_decode: operations.append("DECODIFICA AVANZATA") 
            if do_organize: operations.append("ORGANIZZAZIONE AVANZATA")
            
            print(f"🔧 Operazioni: {' + '.join(operations)}")
            
            risposta = input("\n❓ Continuare? [s/N]: ").strip().lower()
            if risposta not in ['s', 'si', 'sì', 'y', 'yes']:
                print("❌ Operazione annullata")
                return

            result = {}
            success = True
            all_processing_results = []

            # FASE 1: Download
            if do_download:
                print("\n📥 FASE 1: Scaricamento da AdE")
                download_results = system.download_only(data_inizio, data_fine)
                result['download_results'] = download_results
                
                if not download_results:
                    print("❌ Nessun file scaricato")
                    success = False
                else:
                    total_downloads = sum(len(results) for results in download_results.values())
                    successful_downloads = sum(sum(1 for r in results if r.success) 
                                             for results in download_results.values())
                    print(f"✅ Download completato: {successful_downloads}/{total_downloads}")

            # FASE 2: Decodifica Avanzata
            if success and do_decode:
                print("\n🔓 FASE 2: Decodifica P7M Avanzata")
                decode_results = system.decode_only_advanced()
                result['decode_results'] = decode_results
                
                if decode_results.get('success', False):
                    decoded_count = decode_results.get('decoded_files', 0)
                    method_stats = decode_results.get('method_statistics', {})
                    print(f"✅ Decodifica completata: {decoded_count} file")
                    
                    if args.stats and method_stats:
                        print("📊 Statistiche Decodifica:")
                        for method, count in method_stats.items():
                            print(f"    {method}: {count}")
                else:
                    print(f"❌ Decodifica fallita: {decode_results.get('error', 'Errore sconosciuto')}")
                    success = False

            # FASE 3: Organizzazione Avanzata
            if success and do_organize:
                print("\n📁 FASE 3: Organizzazione Avanzata")
                
                if do_download and 'download_results' in result:
                    org_results = system.organizer.organize_downloaded_files(result['download_results'], do_decode)
                else:
                    org_results = system.organize_only_advanced(decode=do_decode)
                
                result['organization_results'] = org_results
                
                if org_results.success:
                    print(f"✅ Organizzazione completata: {org_results.organized_files} file")
                    
                    if args.stats and org_results.decoding_stats:
                        print("📊 Statistiche Organizzazione:")
                        for method, count in org_results.decoding_stats.items():
                            print(f"    {method}: {count}")
                    
                    if org_results.client_folders_created:
                        print(f"\n📁 STRUTTURA CREATA:")
                        for client_id, folders in org_results.client_folders_created.items():
                            client_name = system.config_manager.get_active_clients().get(client_id, {}).get('nome_azienda', client_id)
                            print(f"  👤 {client_name}:")
                            for folder in folders[:3]:
                                print(f"    📂 {folder}")
                            if len(folders) > 3:
                                print(f"    📂 ... e altre {len(folders)-3} cartelle")
                else:
                    print(f"❌ Organizzazione fallita: {len(org_results.errors)} errori")
                    success = False

            # Report finale avanzato
            if success:
                print(f"\n🎉 OPERAZIONI COMPLETATE CON SUCCESSO!")
                
                # Ottieni statistiche complete
                processing_stats = system.organizer.get_processing_statistics()
                
                if args.stats:
                    print("\n📈 STATISTICHE COMPLETE:")
                    if processing_stats.get('decoder'):
                        decoder_stats = processing_stats['decoder']
                        print("🔓 Decoder:")
                        for method, count in decoder_stats.items():
                            print(f"    {method}: {count}")
                    
                    if processing_stats.get('duplicates'):
                        dup_stats = processing_stats['duplicates']
                        print("🔄 Duplicati:")
                        print(f"    File processati: {dup_stats.get('total_processed', 0)}")
                        print(f"    Contenuti unici: {dup_stats.get('unique_content', 0)}")
                
                # Statistiche finali
                if 'download_results' in result:
                    total_downloads = sum(len(results) for results in result['download_results'].values())
                    successful_downloads = sum(sum(1 for r in results if r.success) 
                                             for results in result['download_results'].values())
                    print(f"📊 Download: {successful_downloads}/{total_downloads} file")
                
                if 'decode_results' in result:
                    decoded_count = result['decode_results'].get('decoded_files', 0)
                    print(f"📊 Decodificati: {decoded_count} file")
                
                if 'organization_results' in result:
                    organized_count = result['organization_results'].organized_files
                    print(f"📊 Organizzati: {organized_count} file")
                    
            else:
                print(f"\n❌ OPERAZIONI FALLITE")
                # Mostra errori specifici
                all_errors = []
                if 'decode_results' in result and 'errors' in result['decode_results']:
                    all_errors.extend(result['decode_results']['errors'][:3])
                if 'organization_results' in result:
                    all_errors.extend(result['organization_results'].errors[:3])
                
                if all_errors:
                    print("📝 Errori principali:")
                    for error in all_errors:
                        print(f"  ❗ {error}")

        elif args.command == 'decode-advanced':
            source_dir = Path(args.source) if args.source else None
            print(f"\n🔓 DECODIFICA P7M AVANZATA")
            if source_dir:
                print(f"📂 Directory: {source_dir}")
            
            result = system.decode_only_advanced(source_dir)
            
            if result.get('success'):
                decoded_count = result.get('decoded_files', 0)
                errors_count = len(result.get('errors', []))
                method_stats = result.get('method_statistics', {})
                
                print(f"✅ Decodifica completata: {decoded_count} file, {errors_count} errori")
                
                if args.stats and method_stats:
                    print("\n📊 STATISTICHE METODI:")
                    for method, count in method_stats.items():
                        print(f"  {method}: {count}")
                    
                    file_distribution = result.get('file_method_distribution', {})
                    if file_distribution:
                        print("\n📈 DISTRIBUZIONE PER FILE:")
                        for method, count in file_distribution.items():
                            print(f"  {method}: {count} file")
                        
            else:
                print(f"❌ Decodifica fallita: {result.get('error', 'Errore sconosciuto')}")

        elif args.command == 'organize-advanced':
            source_dir = Path(args.source) if args.source else None
            decode_during_org = not args.no_decode
            
            print(f"\n📁 RIORGANIZZAZIONE AVANZATA")
            if source_dir:
                print(f"📂 Directory: {source_dir}")
            print(f"🔓 Decodifica: {'SÌ' if decode_during_org else 'NO'}")
            
            result = system.organize_only_advanced(source_dir, decode_during_org)
            
            if result.success:
                print(f"✅ Riorganizzazione completata: {result.organized_files} file")
                
                if args.stats and result.decoding_stats:
                    print("\n📊 STATISTICHE DECODIFICA:")
                    for method, count in result.decoding_stats.items():
                        print(f"  {method}: {count}")
                
                if result.client_folders_created:
                    print(f"\n📁 STRUTTURA CREATA:")
                    for client_id, folders in result.client_folders_created.items():
                        print(f"  👤 {client_id}:")
                        for folder in folders[:3]:
                            print(f"    📂 {folder}")
                        if len(folders) > 3:
                            print(f"    📂 ... e altre {len(folders)-3} cartelle")
            else:
                print("❌ Riorganizzazione fallita")
                for error in result.errors[:5]:
                    print(f"  💥 {error}")

    except KeyboardInterrupt:
        print("\n⏹️ Interrotto dall'utente")
    except Exception as e:
        print(f"\n💥 ERRORE CRITICO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()