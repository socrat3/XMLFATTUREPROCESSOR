#!/usr/bin/env python3
"""
SCRIPT DIAGNOSTICA E RESET SISTEMA FATTURE ELETTRONICHE
Per Commercialisti - Versione 1.0

Questo script permette di:
1. Diagnosticare lo stato attuale del sistema
2. Fare backup di sicurezza dei dati esistenti
3. Azzerare completamente la configurazione
4. Preparare ambiente pulito per test
5. Ripristinare da backup se necessario
"""

import os
import shutil
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import logging

# --- CONFIGURAZIONE ---
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('diagnostic_reset.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Percorsi del sistema (devono corrispondere allo script principale)
SYSTEM_PATHS = {
    'INPUT_DIR': Path("fatture_da_processare"),
    'OUTPUT_BASE_DIR': Path("aziende_processate"),
    'ARCHIVE_DIR': Path("archivio_input"),
    'BACKUP_DIR': Path("backup_sistema"),
    'TEMP_DIR': Path("temp_diagnostica")
}

# File di sistema da controllare
SYSTEM_FILES = [
    'invoice_processor.log',
    'diagnostic_reset.log',
    'invoice_processor_enhanced.py'
]

def print_header(title: str):
    """Stampa header colorato."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}")
    print(f"{title}")
    print(f"{'='*80}{Colors.RESET}")

def print_section(title: str):
    """Stampa sezione colorata."""
    print(f"\n{Colors.BOLD}{Colors.YELLOW}--- {title} ---{Colors.RESET}")

def calculate_directory_size(directory: Path) -> tuple[int, int]:
    """Calcola dimensione totale e numero file in una directory."""
    total_size = 0
    file_count = 0
    
    if not directory.exists():
        return 0, 0
        
    try:
        for file_path in directory.rglob('*'):
            if file_path.is_file():
                total_size += file_path.stat().st_size
                file_count += 1
    except PermissionError:
        logger.warning(f"Permessi insufficienti per accedere a {directory}")
    except Exception as e:
        logger.warning(f"Errore calcolo dimensione {directory}: {e}")
        
    return total_size, file_count

def format_size(size_bytes: int) -> str:
    """Formatta dimensione in bytes in formato leggibile."""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    size = float(size_bytes)
    
    while size >= 1024.0 and i < len(size_names) - 1:
        size /= 1024.0
        i += 1
        
    return f"{size:.1f} {size_names[i]}"

def diagnose_system_state() -> Dict:
    """Esegue diagnostica completa dello stato del sistema."""
    print_section("DIAGNOSTICA STATO SISTEMA")
    
    diagnosis = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'directories': {},
        'files': {},
        'summary': {}
    }
    
    # Controlla directory principali
    total_files = 0
    total_size = 0
    
    for name, path in SYSTEM_PATHS.items():
        size, files = calculate_directory_size(path)
        total_files += files
        total_size += size
        
        status = "✅ ESISTENTE" if path.exists() else "❌ MANCANTE"
        
        diagnosis['directories'][name] = {
            'path': str(path),
            'exists': path.exists(),
            'size_bytes': size,
            'size_formatted': format_size(size),
            'file_count': files
        }
        
        print(f"📁 {name:15} {status:12} {format_size(size):>10} ({files} file)")
    
    # Controlla file di sistema
    print(f"\n{Colors.BOLD}File di Sistema:{Colors.RESET}")
    for file_name in SYSTEM_FILES:
        file_path = Path(file_name)
        exists = file_path.exists()
        size = file_path.stat().st_size if exists else 0
        
        status = "✅ PRESENTE" if exists else "❌ MANCANTE"
        
        diagnosis['files'][file_name] = {
            'exists': exists,
            'size_bytes': size,
            'size_formatted': format_size(size)
        }
        
        print(f"📄 {file_name:25} {status:12} {format_size(size):>10}")
    
    # Summary
    diagnosis['summary'] = {
        'total_files': total_files,
        'total_size_bytes': total_size,
        'total_size_formatted': format_size(total_size),
        'directories_exist': sum(1 for d in diagnosis['directories'].values() if d['exists']),
        'files_exist': sum(1 for f in diagnosis['files'].values() if f['exists'])
    }
    
    print(f"\n{Colors.BOLD}Riepilogo:{Colors.RESET}")
    print(f"  • File totali nel sistema: {total_files}")
    print(f"  • Spazio occupato totale: {format_size(total_size)}")
    print(f"  • Directory esistenti: {diagnosis['summary']['directories_exist']}/{len(SYSTEM_PATHS)}")
    print(f"  • File sistema esistenti: {diagnosis['summary']['files_exist']}/{len(SYSTEM_FILES)}")
    
    return diagnosis

def create_backup() -> Optional[Path]:
    """Crea backup completo del sistema."""
    print_section("CREAZIONE BACKUP DI SICUREZZA")
    
    try:
        # Crea directory backup con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_sistema_{timestamp}"
        backup_path = SYSTEM_PATHS['BACKUP_DIR'] / backup_name
        backup_path.mkdir(parents=True, exist_ok=True)
        
        backup_size = 0
        backup_files = 0
        
        # Backup directory dati
        for name, source_path in SYSTEM_PATHS.items():
            if name == 'BACKUP_DIR' or name == 'TEMP_DIR':
                continue  # Non fare backup delle directory di backup e temp
                
            if source_path.exists():
                target_path = backup_path / name
                print(f"📦 Backup {name}...")
                
                if source_path.is_dir():
                    shutil.copytree(source_path, target_path, dirs_exist_ok=True)
                else:
                    shutil.copy2(source_path, target_path)
                
                size, files = calculate_directory_size(target_path)
                backup_size += size
                backup_files += files
                
                print(f"   ✅ Completato: {format_size(size)} ({files} file)")
        
        # Backup file di sistema
        print(f"📦 Backup file di sistema...")
        system_files_backup = backup_path / "system_files"
        system_files_backup.mkdir(exist_ok=True)
        
        for file_name in SYSTEM_FILES:
            file_path = Path(file_name)
            if file_path.exists():
                shutil.copy2(file_path, system_files_backup / file_name)
                backup_files += 1
                backup_size += file_path.stat().st_size
        
        # Crea manifesto backup
        manifest = {
            'backup_name': backup_name,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'total_files': backup_files,
            'total_size_bytes': backup_size,
            'total_size_formatted': format_size(backup_size),
            'directories_backed_up': [name for name, path in SYSTEM_PATHS.items() 
                                    if name not in ['BACKUP_DIR', 'TEMP_DIR'] and path.exists()],
            'system_files_backed_up': [f for f in SYSTEM_FILES if Path(f).exists()]
        }
        
        manifest_path = backup_path / "backup_manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        
        # Crea anche un ZIP del backup
        zip_path = SYSTEM_PATHS['BACKUP_DIR'] / f"{backup_name}.zip"
        print(f"📦 Creazione archivio ZIP...")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in backup_path.rglob('*'):
                if file_path.is_file():
                    arcname = file_path.relative_to(backup_path)
                    zipf.write(file_path, arcname)
        
        zip_size = zip_path.stat().st_size
        
        print(f"\n{Colors.GREEN}✅ BACKUP COMPLETATO CON SUCCESSO!{Colors.RESET}")
        print(f"📁 Directory backup: {backup_path}")
        print(f"📦 Archivio ZIP: {zip_path}")
        print(f"📊 File nel backup: {backup_files}")
        print(f"💾 Dimensione backup: {format_size(backup_size)}")
        print(f"💾 Dimensione ZIP: {format_size(zip_size)}")
        
        return backup_path
        
    except Exception as e:
        logger.error(f"ERRORE durante creazione backup: {e}")
        print(f"{Colors.RED}❌ ERRORE durante creazione backup: {e}{Colors.RESET}")
        return None

def reset_system(skip_backup: bool = False) -> bool:
    """Azzera completamente il sistema."""
    print_section("RESET COMPLETO SISTEMA")
    
    if not skip_backup:
        print("🔄 Creazione backup automatico prima del reset...")
        backup_path = create_backup()
        if not backup_path:
            response = input(f"{Colors.YELLOW}Backup fallito. Continuare comunque? (s/N): {Colors.RESET}")
            if response.lower() != 's':
                print("Reset annullato.")
                return False
    
    try:
        reset_summary = {
            'directories_removed': [],
            'files_removed': [],
            'errors': []
        }
        
        # Rimuove directory principali (tranne backup)
        for name, path in SYSTEM_PATHS.items():
            if name in ['BACKUP_DIR']:  # Non rimuovere i backup
                continue
                
            if path.exists():
                try:
                    print(f"🗑️  Rimozione {name}: {path}")
                    shutil.rmtree(path)
                    reset_summary['directories_removed'].append(str(path))
                    print(f"   ✅ Rimossa")
                except Exception as e:
                    error_msg = f"Errore rimozione {path}: {e}"
                    reset_summary['errors'].append(error_msg)
                    logger.error(error_msg)
                    print(f"   ❌ Errore: {e}")
            else:
                print(f"⏭️  {name} non esiste, saltata")
        
        # Rimuove file di log (mantiene gli script)
        log_files = ['invoice_processor.log', 'diagnostic_reset.log']
        for file_name in log_files:
            file_path = Path(file_name)
            if file_path.exists():
                try:
                    file_path.unlink()
                    reset_summary['files_removed'].append(file_name)
                    print(f"🗑️  File rimosso: {file_name}")
                except Exception as e:
                    error_msg = f"Errore rimozione {file_name}: {e}"
                    reset_summary['errors'].append(error_msg)
                    logger.error(error_msg)
        
        # Crea directory pulite per il test
        print(f"\n🏗️  Creazione directory pulite per test...")
        essential_dirs = ['INPUT_DIR', 'OUTPUT_BASE_DIR', 'ARCHIVE_DIR']
        for name in essential_dirs:
            path = SYSTEM_PATHS[name]
            path.mkdir(parents=True, exist_ok=True)
            print(f"   ✅ Creata: {path}")
        
        # Salva report reset
        reset_report_path = SYSTEM_PATHS['BACKUP_DIR'] / f"reset_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        reset_report_path.parent.mkdir(parents=True, exist_ok=True)
        
        reset_summary['completed_at'] = datetime.now(timezone.utc).isoformat()
        reset_summary['success'] = len(reset_summary['errors']) == 0
        
        with open(reset_report_path, 'w', encoding='utf-8') as f:
            json.dump(reset_summary, f, ensure_ascii=False, indent=2)
        
        if reset_summary['errors']:
            print(f"\n{Colors.YELLOW}⚠️  RESET COMPLETATO CON ALCUNI ERRORI{Colors.RESET}")
            print(f"❌ Errori: {len(reset_summary['errors'])}")
            for error in reset_summary['errors']:
                print(f"   • {error}")
        else:
            print(f"\n{Colors.GREEN}✅ RESET COMPLETATO CON SUCCESSO!{Colors.RESET}")
        
        print(f"📄 Report reset salvato: {reset_report_path}")
        print(f"🗑️  Directory rimosse: {len(reset_summary['directories_removed'])}")
        print(f"🗑️  File rimossi: {len(reset_summary['files_removed'])}")
        
        return True
        
    except Exception as e:
        logger.error(f"ERRORE CRITICO durante reset: {e}")
        print(f"{Colors.RED}❌ ERRORE CRITICO durante reset: {e}{Colors.RESET}")
        return False

def list_backups() -> List[Dict]:
    """Lista tutti i backup disponibili."""
    print_section("BACKUP DISPONIBILI")
    
    backup_dir = SYSTEM_PATHS['BACKUP_DIR']
    if not backup_dir.exists():
        print("📁 Nessuna directory backup trovata")
        return []
    
    backups = []
    
    # Cerca directory backup
    for item in backup_dir.iterdir():
        if item.is_dir() and item.name.startswith('backup_sistema_'):
            manifest_path = item / 'backup_manifest.json'
            if manifest_path.exists():
                try:
                    with open(manifest_path, 'r', encoding='utf-8') as f:
                        manifest = json.load(f)
                    
                    size, files = calculate_directory_size(item)
                    manifest['actual_size'] = format_size(size)
                    manifest['actual_files'] = files
                    manifest['backup_path'] = str(item)
                    
                    backups.append(manifest)
                    
                except Exception as e:
                    logger.warning(f"Errore lettura manifest {manifest_path}: {e}")
    
    # Cerca anche file ZIP
    for item in backup_dir.iterdir():
        if item.is_file() and item.name.endswith('.zip') and item.name.startswith('backup_sistema_'):
            zip_size = item.stat().st_size
            timestamp_str = item.name.replace('backup_sistema_', '').replace('.zip', '')
            
            backups.append({
                'backup_name': item.stem,
                'created_at': f"ZIP: {timestamp_str}",
                'backup_path': str(item),
                'actual_size': format_size(zip_size),
                'is_zip': True
            })
    
    # Ordina per data di creazione
    backups.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    
    if not backups:
        print("📦 Nessun backup trovato")
        return []
    
    print(f"📦 Trovati {len(backups)} backup:")
    for i, backup in enumerate(backups, 1):
        created_at = backup.get('created_at', 'N/D')
        if not backup.get('is_zip', False):
            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"  {i}. {backup['backup_name']}")
        print(f"     📅 Creato: {created_at}")
        print(f"     💾 Dimensione: {backup['actual_size']}")
        if not backup.get('is_zip', False):
            print(f"     📊 File: {backup.get('actual_files', 'N/D')}")
        print(f"     📁 Percorso: {backup['backup_path']}")
        print()
    
    return backups

def create_test_environment():
    """Crea ambiente di test con file di esempio."""
    print_section("CREAZIONE AMBIENTE DI TEST")
    
    # Crea directory di input se non esiste
    input_dir = SYSTEM_PATHS['INPUT_DIR']
    input_dir.mkdir(parents=True, exist_ok=True)
    
    # Crea file README per test
    readme_content = """
# DIRECTORY TEST FATTURE ELETTRONICHE

Questa directory è stata preparata per testare il sistema di elaborazione fatture.

## Come testare:

1. Inserire i file delle fatture elettroniche (.xml, .p7m, .zip) in questa cartella
2. Eseguire lo script principale: python invoice_processor_enhanced.py
3. Controllare i risultati nella cartella 'aziende_processate'
4. Verificare il report generato

## Struttura file supportati:
- Fatture in formato P7M (firmato digitalmente)
- Fatture in formato XML (non firmato)
- Archivi ZIP contenenti fatture
- File metadati associati

## Note:
- Lo script organizzerà automaticamente per azienda/anno/direzione
- Genererà report dettagliati dell'elaborazione
- Eviterà duplicati usando hash MD5 e ID fattura
"""
    
    readme_path = input_dir / "README_TEST.txt"
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"✅ Directory di test creata: {input_dir}")
    print(f"📄 File README creato: {readme_path}")
    print(f"\n{Colors.GREEN}🚀 AMBIENTE DI TEST PRONTO!{Colors.RESET}")
    print(f"   1. Inserire fatture elettroniche in: {input_dir}")
    print(f"   2. Eseguire: python invoice_processor_enhanced.py")
    print(f"   3. Controllare risultati in: {SYSTEM_PATHS['OUTPUT_BASE_DIR']}")

def interactive_menu():
    """Menu interattivo per le operazioni."""
    print_header("SISTEMA DIAGNOSTICA E RESET - FATTURE ELETTRONICHE")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    while True:
        print(f"\n{Colors.BOLD}MENU PRINCIPALE:{Colors.RESET}")
        print("1. 🔍 Diagnostica stato sistema")
        print("2. 📦 Crea backup completo")
        print("3. 🗑️  Reset completo sistema")
        print("4. 📋 Lista backup disponibili")
        print("5. 🚀 Crea ambiente di test pulito")
        print("6. ❌ Esci")
        
        try:
            choice = input(f"\n{Colors.CYAN}Seleziona opzione (1-6): {Colors.RESET}").strip()
            
            if choice == '1':
                diagnose_system_state()
                
            elif choice == '2':
                create_backup()
                
            elif choice == '3':
                print(f"\n{Colors.RED}⚠️  ATTENZIONE: Questa operazione cancellerà TUTTI i dati!{Colors.RESET}")
                print("Verrà creato un backup automatico prima del reset.")
                confirm = input(f"{Colors.YELLOW}Confermi il reset completo? (RESET/n): {Colors.RESET}")
                
                if confirm == "RESET":
                    reset_system()
                else:
                    print("Reset annullato.")
                    
            elif choice == '4':
                list_backups()
                
            elif choice == '5':
                create_test_environment()
                
            elif choice == '6':
                print(f"{Colors.GREEN}👋 Arrivederci!{Colors.RESET}")
                break
                
            else:
                print(f"{Colors.RED}❌ Opzione non valida. Riprova.{Colors.RESET}")
                
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Operazione interrotta dall'utente.{Colors.RESET}")
            break
        except Exception as e:
            logger.error(f"Errore nel menu: {e}")
            print(f"{Colors.RED}❌ Errore: {e}{Colors.RESET}")

def main():
    """Funzione principale."""
    try:
        # Se chiamato con argomenti, esegue operazione specifica
        import sys
        if len(sys.argv) > 1:
            if sys.argv[1] == '--diagnose':
                diagnose_system_state()
            elif sys.argv[1] == '--backup':
                create_backup()
            elif sys.argv[1] == '--reset':
                print("Reset con parametro --reset richiede conferma manuale")
                print("Usa il menu interattivo per maggiore sicurezza")
            elif sys.argv[1] == '--test-env':
                create_test_environment()
            else:
                print("Parametri disponibili: --diagnose, --backup, --test-env")
        else:
            # Menu interattivo
            interactive_menu()
            
    except Exception as e:
        logger.critical(f"ERRORE CRITICO: {e}")
        print(f"{Colors.RED}❌ ERRORE CRITICO: {e}{Colors.RESET}")

if __name__ == "__main__":
    main()