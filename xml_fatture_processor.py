import os
import datetime
import sys
import subprocess
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from xml.etree.ElementTree import parse
from fpdf import FPDF, XPos, YPos
from prettytable import PrettyTable

# Versione del software
VERSION = "1.7.3 del 02-02-2025"

pdf_file_path = "fatture.pdf"
# Dizionario per tradurre i mesi in italiano
mesi_italiani = {
    1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile", 5: "Maggio", 6: "Giugno",
    7: "Luglio", 8: "Agosto", 9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
}

@dataclass
class Fattura:
    cedente_id_fiscale: str
    cedente_partita_iva: str
    cedente_denominazione: str
    cessionario_id_fiscale: str
    cessionario_partita_iva: str
    cessionario_denominazione: str
    data: datetime.date
    numero: str
    ritenuta_applicata: bool
    importo_ritenuta: float
    stato_elaborazione: str
    nome_file: str

def truncate_string(value: str, max_length: int = 25) -> str:
    return value if len(value) <= max_length else value[:max_length] + "..."

def get_denominazione_or_nome_cognome(node):
    denominazione = node.find('Denominazione')
    if denominazione is not None and denominazione.text:
        return denominazione.text

    nome = node.find('Nome')
    cognome = node.find('Cognome')
    if nome is not None and cognome is not None:
        return f"{nome.text} {cognome.text}"

    return "Dati anagrafici mancanti"

def process_file(file_path: str, file_name: str) -> Fattura:
    try:
        tree = parse(file_path)
        root = tree.getroot()

        header = root.find('FatturaElettronicaHeader')
        cedente = header.find('CedentePrestatore')
        cessionario = header.find('CessionarioCommittente')
        body = root.find('FatturaElettronicaBody')
        dati_generali = body.find('DatiGenerali')
        dati_generali_documento = dati_generali.find('DatiGeneraliDocumento')

        cedente_id_fiscale = cedente.find('DatiAnagrafici/CodiceFiscale')
        cedente_id_fiscale = cedente_id_fiscale.text if cedente_id_fiscale is not None else "NO_CF_cedente"

        cedente_partita_iva = cedente.find('DatiAnagrafici/IdFiscaleIVA/IdCodice')
        cedente_partita_iva = cedente_partita_iva.text if cedente_partita_iva is not None else "NO_PIVA_cedente"
        cedente_denominazione = get_denominazione_or_nome_cognome(cedente.find('DatiAnagrafici/Anagrafica'))

        cessionario_id_fiscale = cessionario.find('DatiAnagrafici/CodiceFiscale')
        cessionario_id_fiscale = cessionario_id_fiscale.text if cessionario_id_fiscale is not None else "NO_CF_cessionario"

        cessionario_partita_iva = cessionario.find('DatiAnagrafici/IdFiscaleIVA/IdCodice')
        cessionario_partita_iva = cessionario_partita_iva.text if cessionario_partita_iva is not None else "NO_PIVA_cessionario"
        cessionario_denominazione = get_denominazione_or_nome_cognome(cessionario.find('DatiAnagrafici/Anagrafica'))

        data = datetime.datetime.strptime(dati_generali_documento.find('Data').text, '%Y-%m-%d').date()
        numero = dati_generali_documento.find('Numero').text

        ritenuta = dati_generali_documento.find('DatiRitenuta')
        importo_ritenuta = float(ritenuta.find('ImportoRitenuta').text) if ritenuta is not None else 0.0
        has_ritenuta = importo_ritenuta > 0

        return Fattura(
            cedente_id_fiscale=cedente_id_fiscale,
            cedente_partita_iva=cedente_partita_iva,
            cedente_denominazione=cedente_denominazione,
            cessionario_id_fiscale=cessionario_id_fiscale,
            cessionario_partita_iva=cessionario_partita_iva,
            cessionario_denominazione=cessionario_denominazione,
            data=data,
            numero=numero,
            ritenuta_applicata=has_ritenuta,
            importo_ritenuta=importo_ritenuta,
            stato_elaborazione="OK",
            nome_file=file_name
        )
    except Exception as e:
        return Fattura(
            cedente_id_fiscale="",
            cedente_partita_iva="",
            cedente_denominazione="",
            cessionario_id_fiscale="",
            cessionario_partita_iva="",
            cessionario_denominazione="",
            data=datetime.date.today(),
            numero="",
            ritenuta_applicata=False,
            importo_ritenuta=0.0,
            stato_elaborazione=f"KO: {e}",
            nome_file=file_name
        )

def decode_p7m_files(input_file_path: str):
    # Verifica che il percorso di input esista e sia una directory
    if not os.path.exists(input_file_path) or not os.path.isdir(input_file_path):
        raise FileNotFoundError(f"La cartella di input '{input_file_path}' non esiste o non è una directory.")

    # Verifica se la cartella contiene file
    if not os.listdir(input_file_path):
        print("La cartella di input è vuota.")
    else:
        # Elabora i file .p7m nella cartella
        for filename in os.listdir(input_file_path):
            if filename.lower().endswith(".p7m") and "metadato" not in filename.lower():
                file_path_p7m = os.path.join(input_file_path, filename)
                split_filename = os.path.splitext(filename)  # Divide il nome dal suffisso
                file_name_decript = os.path.join(input_file_path, split_filename[0])  # Nome senza estensione .p7m

                # Comando OpenSSL per decrittare e verificare il file
                command = (
                    f"openssl cms -decrypt -verify -inform DER -in \"{file_path_p7m}\" -noverify -out \"{file_name_decript}\""
                )
                print(f"Eseguendo comando: {command}")

                try:
                    subprocess.run(command, shell=True, check=True)
                    print(f"File decrittato con successo: {file_name_decript}")

                    # Rimuove il file .p7m dopo la decodifica
                    os.remove(file_path_p7m)
                    print(f"File .p7m eliminato: {file_path_p7m}")
                except subprocess.CalledProcessError as e:
                    print(f"Errore durante la decrittazione del file {filename}: {e}")

def read_fatture(folder_path: str) -> List[Fattura]:
    fatture = []
    fatture_table = PrettyTable(["#", "Nome Fattura", "Fornitore", "Cliente", "Numero", "Data", "Ritenuta", "Importo Ritenuta", "Elaborazione"])

    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith(('.xml', '.p7m')) and "metadato" not in file_name.lower():
                file_path = os.path.join(root, file_name)
                fattura = process_file(file_path, file_name)
                fatture.append(fattura)

    for idx, fattura in enumerate(fatture, start=1):
        fatture_table.add_row([
            idx, fattura.nome_file, truncate_string(fattura.cedente_denominazione), truncate_string(fattura.cessionario_denominazione), fattura.numero,
            fattura.data.strftime('%d/%m/%Y') if fattura.stato_elaborazione == "OK" else "-",
            "SI" if fattura.ritenuta_applicata else "NO",
            f"{fattura.importo_ritenuta:.2f}" if fattura.ritenuta_applicata else "-",
            fattura.stato_elaborazione
        ])

    print(fatture_table)
    return fatture

def aggregate_by_supplier_and_client(fatture: List[Fattura]) -> Dict[str, Dict[Tuple[int, int], List[Fattura]]]:
    aggregato = {}
    for fattura in fatture:
        key = fattura.cedente_denominazione
        year_month = (fattura.data.year, fattura.data.month)
        if key not in aggregato:
            aggregato[key] = {}
        if year_month not in aggregato[key]:
            aggregato[key][year_month] = []
        aggregato[key][year_month].append(fattura)
    return aggregato

def filter_fatture_by_date_and_ritenuta(fatture: List[Fattura], start_date: Optional[datetime.date], end_date: Optional[datetime.date]) -> List[Fattura]:
    filtered_fatture = []
    for fattura in fatture:
        if fattura.ritenuta_applicata:
            if start_date and end_date:
                if start_date <= fattura.data <= end_date:
                    filtered_fatture.append(fattura)
            elif start_date:
                if fattura.data >= start_date:
                    filtered_fatture.append(fattura)
            elif end_date:
                if fattura.data <= end_date:
                    filtered_fatture.append(fattura)
            else:
                filtered_fatture.append(fattura)
    return filtered_fatture

def filter_fatture_by_partita_iva(fatture: List[Fattura], partita_iva: str, is_fornitore: bool) -> List[Fattura]:
    filtered_fatture = []
    for fattura in fatture:
        if is_fornitore and fattura.cedente_partita_iva == partita_iva:
            filtered_fatture.append(fattura)
        elif not is_fornitore and fattura.cessionario_partita_iva == partita_iva:
            filtered_fatture.append(fattura)
    return filtered_fatture

def export_to_pdf(fatture: List[Fattura], pdf_file_path: str, start_date: Optional[datetime.date], end_date: Optional[datetime.date], save_output: bool = False, output_text: str = ""):
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_font("Helvetica", size=10)

    # Intestazione del programma
    pdf.add_page()
    pdf.set_font("Helvetica", style="B", size=12)
    pdf.cell(0, 10, f"XML Fatture Processor {VERSION} di S.re Crapanzano - Licenza GNU-GPL - Agrigento Città della Cultura 2025", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
    pdf.ln(1)

    # Specifica del periodo se indicato
    if start_date and end_date:
        periodo = f"Periodo di elaborazione dal {start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}"
        pdf.cell(0, 10, periodo, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        pdf.ln(1)

    oggi = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    grouped_fatture = aggregate_by_supplier_and_client(fatture)

    # Aggiungi il riepilogo totale delle ritenute nella prima pagina
    pdf.set_font("Helvetica", style="B", size=12)
    pdf.cell(0, 10, "Riepilogo totale ritenute per Fornitore e Cliente:", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    pdf.set_font("Helvetica", size=10)
    pdf.ln(1)

    for fornitore, fatture_per_mese in grouped_fatture.items():
        totale_periodo_tutto = sum(f.importo_ritenuta for f in fatture if f.ritenuta_applicata and (not start_date or f.data >= start_date) and (not end_date or f.data <= end_date))
        totale_periodo_fornitore = sum(f.importo_ritenuta for f in filtered_fatture if f.ritenuta_applicata and f.cedente_denominazione == fornitore and (not start_date or f.data >= start_date) and (not end_date or f.data <= end_date))
        # Imposta il font in grassetto
        pdf.set_font("Helvetica", style="B", size=10)
        pdf.cell(0, 10, f"Fornitore: {fornitore} (P.IVA: {fatture_per_mese[next(iter(fatture_per_mese))][0].cedente_partita_iva})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        pdf.set_font("Helvetica", size=10)
        pdf.cell(0, 10, f"Totale Ritenute per il {periodo} Euro {totale_periodo_tutto:.2f} di cui per il fornitore Euro {totale_periodo_fornitore:.2f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        pdf.ln(1)

        # Ordina le fatture per anno e mese
        sorted_year_months = sorted(fatture_per_mese.keys())
        for year_month in sorted_year_months:
            fatture_gruppo = fatture_per_mese[year_month]
            fatture_gruppo.sort(key=lambda x: x.data)
            totale_mese = sum(f.importo_ritenuta for f in fatture_gruppo if f.ritenuta_applicata)
            pdf.cell(0, 10, f"  Mese: {mesi_italiani[year_month[1]]} {year_month[0]}, Totale Ritenute: {totale_mese:.2f} Euro", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")

    pdf.ln(20)

    for fornitore, fatture_per_mese in grouped_fatture.items():
        clienti = set(f"{f.cessionario_denominazione} (COD.FISC. {f.cessionario_id_fiscale} Part.IVA {f.cessionario_partita_iva})" for f in fatture if f.cedente_denominazione == fornitore)
        clienti_str = ", ".join(clienti)
        pdf.add_page()
        pdf.set_font("Helvetica", style="B", size=10)
        pdf.cell(0, 10, f"FORNITORE: {fornitore.upper()} (P.IVA/C.F.: {fatture_per_mese[next(iter(fatture_per_mese))][0].cedente_id_fiscale})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        pdf.set_font("Helvetica", size=10)
        pdf.cell(0, 10, f"Clienti: {clienti_str}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        pdf.cell(0, 10, f"Data e ora elaborazione: {oggi}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        if start_date and end_date:
            pdf.cell(0, 10, f"Periodo trattato: dal {start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")

        headers = ["#", "Nome Fattura", "Cliente", "Numero", "Data", "Ritenuta", "Importo Ritenuta", "Elaborazione"]
        col_widths = [10, 60, 60, 30, 30, 20, 30, 40]

        pdf.ln()
        for header, width in zip(headers, col_widths):
            pdf.cell(width, 10, header, border=1, align="C")
        pdf.ln()

        idx = 1
        # Ordina le fatture per anno e mese
        sorted_year_months = sorted(fatture_per_mese.keys())
        for year_month in sorted_year_months:
            fatture_gruppo = fatture_per_mese[year_month]
            fatture_gruppo.sort(key=lambda x: x.data)
            for fattura in fatture_gruppo:
                data = [
                    idx, fattura.nome_file, truncate_string(fattura.cessionario_denominazione) + f"\n(P.IVA/C.F.: {fattura.cessionario_id_fiscale})",
                    fattura.numero, fattura.data.strftime('%d/%m/%Y') if fattura.stato_elaborazione == "OK" else "-",
                    "SI" if fattura.ritenuta_applicata else "NO",
                    f"{fattura.importo_ritenuta:.2f}" if fattura.ritenuta_applicata else "-",
                    fattura.stato_elaborazione
                ]
                for value, width in zip(data, col_widths):
                    pdf.cell(width, 10, truncate_string(str(value), max_length=25), border=1, align="C")
                pdf.ln()
                idx += 1

    if save_output:
        pdf.add_page()
        pdf.set_font("Helvetica", size=10)
        pdf.multi_cell(0, 10, output_text)

    pdf.output(pdf_file_path)

def print_syntax_error():
    print(f"Sintassi corretta: python script.py /path/to/fatture [-R] [-M] [-FORNITORE partita_iva] [-CLIENTE partita_iva] [start_date end_date]")
    print(f"Esempio: python script.py /path/to/fatture -R -M -FORNITORE 12345678901 01/01/2024 31/12/2024")
    print("Formato delle date: DD/MM/YYYY")
    print("Formato della partita IVA: 11 numeri")

if __name__ == "__main__":
    print(f"XML Fatture Processor {VERSION} ** Agrigento città della cultura 2025")
    print("Sviluppato da Salvatore Crapanzano")
    print("Rilasciato sotto licenza GNU-GPL")
    print()

    if len(sys.argv) < 2:
        print_syntax_error()
        sys.exit(1)

    folder_path = sys.argv[1]
    filter_option = '-R' in sys.argv
    save_output_option = '-M' in sys.argv
    fornitore_option = '-FORNITORE' in sys.argv
    cliente_option = '-CLIENTE' in sys.argv
    start_date_str = None
    end_date_str = None
    fornitore_partita_iva = None
    cliente_partita_iva = None

    # Rimuovi le opzioni -R, -M, -FORNITORE e -CLIENTE dagli argomenti
    argv = [arg for arg in sys.argv if arg not in ['-R', '-M', '-FORNITORE', '-CLIENTE']]

    if fornitore_option:
        fornitore_index = sys.argv.index('-FORNITORE')
        if fornitore_index + 1 < len(sys.argv):
            fornitore_partita_iva = sys.argv[fornitore_index + 1]
            if len(fornitore_partita_iva) != 11 or not fornitore_partita_iva.isdigit():
                print_syntax_error()
                sys.exit(1)
        else:
            print_syntax_error()
            sys.exit(1)

    if cliente_option:
        cliente_index = sys.argv.index('-CLIENTE')
        if cliente_index + 1 < len(sys.argv):
            cliente_partita_iva = sys.argv[cliente_index + 1]
            if len(cliente_partita_iva) != 11 or not cliente_partita_iva.isdigit():
                print_syntax_error()
                sys.exit(1)
        else:
            print_syntax_error()
            sys.exit(1)

    if filter_option:
        if len(argv) < 4:
            print_syntax_error()
            sys.exit(1)
        start_date_str = argv[2]
        end_date_str = argv[3]
    elif len(argv) > 2:
        start_date_str = argv[2]
        end_date_str = argv[3]

    try:
        start_date = datetime.datetime.strptime(start_date_str, '%d/%m/%Y').date() if start_date_str else None
        end_date = datetime.datetime.strptime(end_date_str, '%d/%m/%Y').date() if end_date_str else None
    except ValueError:
        print_syntax_error()
        sys.exit(1)

    decode_p7m_files(folder_path)
    fatture = read_fatture(folder_path)

    if not start_date or not end_date:
        # Calcola le date minime e massime delle fatture
        date_fatture = [fattura.data for fattura in fatture if fattura.stato_elaborazione == "OK"]
        if date_fatture:
            start_date = min(date_fatture)
            end_date = max(date_fatture)

    if fornitore_partita_iva:
        fatture = filter_fatture_by_partita_iva(fatture, fornitore_partita_iva, is_fornitore=True)

    if cliente_partita_iva:
        fatture = filter_fatture_by_partita_iva(fatture, cliente_partita_iva, is_fornitore=False)

    if filter_option:
        filtered_fatture = filter_fatture_by_date_and_ritenuta(fatture, start_date, end_date)
    else:
        filtered_fatture = fatture

    # Ottieni i nomi del primo cliente e fornitore
    nome_cliente = truncate_string(fatture[0].cessionario_denominazione.replace(" ", "_"), max_length=50) if fatture else "NessunCliente"
    nome_fornitore = truncate_string(fatture[0].cedente_denominazione.replace(" ", "_"), max_length=50) if fatture else "NessunFornitore"

    # Formatta l'intervallo di date
    data_inizio = start_date.strftime('%d%m%Y') if start_date else "Inizio"
    data_fine = end_date.strftime('%d%m%Y') if end_date else "Fine"

    # Crea il nome del file PDF
    pdf_file_name = f"fatture_{nome_cliente}_{data_inizio}_{data_fine}.pdf"

    # Percorso completo del file PDF
    pdf_file_path = os.path.join(folder_path, pdf_file_name)
    print(f"File PDF generato: {pdf_file_path}")

    output_text = ""
    if save_output_option:
        output_text = "Riepilogo Totale Ritenute per Fornitore e Cliente:\n"
        aggregato = aggregate_by_supplier_and_client(filtered_fatture)
        for fornitore, fatture_per_mese in aggregato.items():
            totale_periodo = sum(f.importo_ritenuta for f in filtered_fatture if f.ritenuta_applicata and f.cedente_denominazione == fornitore and (not start_date or f.data >= start_date) and (not end_date or f.data <= end_date))
            output_text += f"Fornitore: {fornitore}, Totale Ritenute per il periodo: {totale_periodo:.2f} Euro\n"
            sorted_year_months = sorted(fatture_per_mese.keys())
            for year_month in sorted_year_months:
                year, month = year_month
                totale_mese = sum(f.importo_ritenuta for f in fatture_per_mese[year_month] if f.ritenuta_applicata)
                output_text += f"  Mese: {mesi_italiani[month]} {year}, Totale Ritenute: {totale_mese:.2f} Euro\n"
        print(output_text)

    export_to_pdf(filtered_fatture, pdf_file_path, start_date, end_date, save_output_option, output_text)

# Apre automaticamente il file PDF appena generato
if os.path.exists(pdf_file_path):
    try:
        if sys.platform == "win32":  # Per Windows
            os.startfile(pdf_file_path)
        else:
            print("Sistema operativo non supportato per questa operazione.")
    except Exception as e:
        print(f"Errore durante l'apertura del file PDF: {e}. Provo con il comando 'start'...")
        try:
            # Metodo alternativo con subprocess
            subprocess.run(["start", "", pdf_file_path], shell=True, check=True)
        except Exception as e2:
            print(f"Errore con il metodo alternativo: {e2}. Controlla il visualizzatore predefinito.")
else:
    print(f"File PDF non trovato: {pdf_file_path}")
