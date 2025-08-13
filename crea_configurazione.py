#!/usr/bin/env python3
"""
Script per creare la configurazione iniziale del sistema AdE
"""

import json
import os

def create_config():
    """Crea configurazione di default."""
    
    print("=== CREAZIONE CONFIGURAZIONE SISTEMA ADE ===")
    print()
    
    # Configurazione completa
    config = {
        "sistema": {
            "nome": "Sistema Integrato Fatture Elettroniche AdE",
            "versione": "2.0",
            "debug_mode": False,
            "conserva_nomi_originali_metadati": True
        },
        "credenziali_ade": {
            "codice_fiscale": "",
            "pin": "",
            "password": "",
            "codice_fiscale_studio": ""
        },
        "portfolio_clienti": {
            "esempio_cliente": {
                "nome_azienda": "Sostituire con nome reale",
                "partita_iva_diretta": "12345678901",
                "codice_fiscale": "12345678901",
                "profilo_accesso": 1,
                "attivo": False,
                "note": "Cliente di esempio - modificare con dati reali"
            }
        },
        "configurazione_download": {
            "modalita": "completa",
            "tipi_documenti": {
                "fatture_emesse": True,
                "fatture_ricevute": True,
                "fatture_passive": True,
                "transfrontaliere_emesse": True,
                "transfrontaliere_ricevute": True
            },
            "download_metadati": True,
            "download_ricevute_sdi": True,
            "criterio_ricerca_ricevute": "entrambi",
            "decodifica_p7m": True,
            "pausa_tra_download": 0.5
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
            "salva_prima_nota": True,
            "reports_retention_days": 90
        },
        "scheduling": {
            "abilitato": False,
            "frequenza": "settimanale",
            "giorno_settimana": "monday",
            "ora_esecuzione": "02:00",
            "periodo_automatico": "ultimo_mese",
            "manutenzione_automatica": True
        },
        "notifiche": {
            "abilitate": False,
            "email": {
                "abilitata": False,
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "username": "",
                "password": "",
                "destinatari": []
            },
            "webhook": {
                "abilitato": False,
                "url": "",
                "timeout": 10
            }
        },
        "logging": {
            "livello": "INFO",
            "file_log": True,
            "console_log": True,
            "max_size_mb": 10,
            "backup_count": 5,
            "retention_days": 30
        }
    }
    
    config_file = "config_ade_system.json"
    
    # Controlla se esiste già
    if os.path.exists(config_file):
        risposta = input(f"Il file {config_file} esiste già. Sovrascrivere? [s/N]: ").strip().lower()
        if risposta not in ['s', 'si', 'sì', 'y', 'yes']:
            print("Operazione annullata.")
            return
    
    try:
        # Salva configurazione
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Configurazione creata: {config_file}")
        print()
        print("🔧 PROSSIMI PASSI:")
        print("1. Apri il file config_ade_system.json")
        print("2. Inserisci le tue credenziali AdE nella sezione 'credenziali_ade'")
        print("3. Configura i tuoi clienti nella sezione 'portfolio_clienti'")
        print("4. Testa con: python xml_fatture_processor_v5.py test-login")
        print()
        print("📋 PROFILI DI ACCESSO:")
        print("   1 = Delega Diretta (più comune)")
        print("   2 = Me stesso")
        print("   3 = Studio Associato")
        
    except Exception as e:
        print(f"❌ Errore creazione configurazione: {e}")

def input_guided_config():
    """Configurazione guidata interattiva."""
    
    print("=== CONFIGURAZIONE GUIDATA ===")
    print()
    
    # Raccoglie dati base
    print("📋 CREDENZIALI AGENZIA DELLE ENTRATE:")
    cf = input("Codice Fiscale: ").strip().upper()
    pin = input("PIN: ").strip()
    password = input("Password: ").strip()
    cf_studio = input("Codice Fiscale Studio (opzionale): ").strip().upper()
    
    print()
    print("🏢 PRIMO CLIENTE:")
    nome_azienda = input("Nome Azienda: ").strip()
    piva = input("Partita IVA: ").strip()
    cf_cliente = input("Codice Fiscale cliente (Enter = usa P.IVA): ").strip() or piva
    
    print()
    print("📋 PROFILO DI ACCESSO:")
    print("1 = Delega Diretta")
    print("2 = Me stesso") 
    print("3 = Studio Associato")
    profilo = input("Scegli profilo [1]: ").strip() or "1"
    
    try:
        profilo = int(profilo)
    except:
        profilo = 1
    
    # Crea configurazione
    config = {
        "credenziali_ade": {
            "codice_fiscale": cf,
            "pin": pin,
            "password": password,
            "codice_fiscale_studio": cf_studio
        },
        "portfolio_clienti": {
            "cliente_001": {
                "nome_azienda": nome_azienda,
                "partita_iva_diretta": piva,
                "codice_fiscale": cf_cliente,
                "profilo_accesso": profilo,
                "attivo": True,
                "note": "Configurazione guidata"
            }
        },
        "configurazione_download": {
            "tipi_documenti": {
                "fatture_emesse": True,
                "fatture_ricevute": True,
                "fatture_passive": True,
                "transfrontaliere_emesse": False,
                "transfrontaliere_ricevute": False
            },
            "download_metadati": True,
            "decodifica_p7m": True,
            "pausa_tra_download": 0.5
        },
        "directory_sistema": {
            "input_temp": "temp_download_ade",
            "output_base": "aziende_processate",
            "archivio": "archivio_input",
            "logs": "logs_sistema",
            "reports": "reports_sistema"
        },
        "scheduling": {
            "abilitato": False
        },
        "logging": {
            "livello": "INFO",
            "file_log": True,
            "console_log": True
        }
    }
    
    config_file = "config_ade_system.json"
    
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        print()
        print(f"✅ Configurazione salvata: {config_file}")
        print("🧪 Testa ora con: python xml_fatture_processor_v5.py test-login")
        
    except Exception as e:
        print(f"❌ Errore salvataggio: {e}")

def main():
    print("🚀 CONFIGURAZIONE SISTEMA ADE")
    print()
    print("Scegli modalità:")
    print("1. Configurazione guidata (raccomandato)")
    print("2. Template vuoto da compilare")
    print("3. Esci")
    
    scelta = input("\nScelta [1]: ").strip() or "1"
    
    if scelta == "1":
        input_guided_config()
    elif scelta == "2":
        create_config()
    else:
        print("Uscita.")

if __name__ == "__main__":
    main()