import os
import datetime
import sys
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from xml.etree.ElementTree import parse
from fpdf import FPDF, XPos, YPos
from prettytable import PrettyTable

@dataclass
class Fattura:
    cedente_id_fiscale: str
    cedente_denominazione: str
    cessionario_denominazione: str
    cessionario_id_fiscale: str
    cessionario_partita_iva: str
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

        cedente_id_fiscale = cedente.find('DatiAnagrafici/IdFiscaleIVA/IdCodice').text
        cedente_denominazione = get_denominazione_or_nome_cognome(cedente.find('DatiAnagrafici/Anagrafica'))
        cessionario_denominazione = get_denominazione_or_nome_cognome(cessionario.find('DatiAnagrafici/Anagrafica'))
        cessionario_id_fiscale = cessionario.find('DatiAnagrafici/IdFiscaleIVA/IdCodice').text
        cessionario_partita_iva = cessionario.find('DatiAnagrafici/IdFiscaleIVA/IdCodice').text
        data = datetime.datetime.strptime(dati_generali_documento.find('Data').text, '%Y-%m-%d').date()
        numero = dati_generali_documento.find('Numero').text

        ritenuta = dati_generali_documento.find('DatiRitenuta')
        importo_ritenuta = float(ritenuta.find('ImportoRitenuta').text) if ritenuta is not None else 0.0
        has_ritenuta = importo_ritenuta > 0

        return Fattura(
            cedente_id_fiscale=cedente_id_fiscale,
            cedente_denominazione=cedente_denominazione,
            cessionario_denominazione=cessionario_denominazione,
            cessionario_id_fiscale=cessionario_id_fiscale,
            cessionario_partita_iva=cessionario_partita_iva,
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
            cedente_denominazione="",
            cessionario_denominazione="",
            cessionario_id_fiscale="",
            cessionario_partita_iva="",
            data=datetime.date.today(),
            numero="",
            ritenuta_applicata=False,
            importo_ritenuta=0.0,
            stato_elaborazione=f"KO: {e}",
            nome_file=file_name
        )

def read_fatture(folder_path: str) -> List[Fattura]:
    fatture = []
    fatture_table = PrettyTable(["#", "Nome Fattura", "Fornitore", "Cliente", "Numero", "Data", "Ritenuta", "Importo Ritenuta", "Elaborazione"])

    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith(('.xml', '.p7m')):
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

def aggregate_by_supplier_and_client(fatture: List[Fattura]) -> Dict[str, List[Fattura]]:
    aggregato = {}
    for fattura in fatture:
        key = fattura.cedente_denominazione
        if key not in aggregato:
            aggregato[key] = []
        aggregato[key].append(fattura)
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

def export_to_pdf(fatture: List[Fattura], pdf_file_path: str):
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_font("Helvetica", size=10)

    # Intestazione del programma
    pdf.add_page()
    pdf.set_font("Helvetica", style="B", size=12)
    pdf.cell(0, 10, "XML Fatture Processor 1.0 del 15/01/2025 di Salvatore Crapanzano - Licenza GNU-GPL", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
    pdf.ln(20)

    oggi = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    grouped_fatture = aggregate_by_supplier_and_client(fatture)

    # Aggiungi il riepilogo totale delle ritenute nella prima pagina
    pdf.set_font("Helvetica", style="B", size=12)
    pdf.cell(0, 10, "Riepilogo Totale Ritenute per Fornitore e Cliente:", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
    pdf.set_font("Helvetica", size=10)
    pdf.ln(10)

    for fornitore, fatture_gruppo in grouped_fatture.items():
        totale = sum(f.importo_ritenuta for f in fatture_gruppo if f.ritenuta_applicata)
        pdf.cell(0, 10, f"Fornitore: {fornitore} (P.IVA/C.F.: {fatture_gruppo[0].cedente_id_fiscale}), Totale Ritenute: {totale:.2f} Euro", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")

    pdf.ln(20)

    for fornitore, fatture_gruppo in grouped_fatture.items():
        fatture_gruppo.sort(key=lambda x: x.data)
        pdf.add_page()
        pdf.set_font("Helvetica", style="B", size=10)
        pdf.cell(0, 10, f"FORNITORE: {fornitore.upper()} (P.IVA/C.F.: {fatture_gruppo[0].cedente_id_fiscale})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")
        pdf.set_font("Helvetica", size=10)
        pdf.cell(0, 10, f"Data e ora elaborazione: {oggi}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="L")

        headers = ["#", "Nome Fattura", "Cliente", "Numero", "Data", "Ritenuta", "Importo Ritenuta", "Elaborazione"]
        col_widths = [10, 60, 60, 30, 30, 20, 30, 40]

        pdf.ln()
        for header, width in zip(headers, col_widths):
            pdf.cell(width, 10, header, border=1, align="C")
        pdf.ln()

        for idx, fattura in enumerate(fatture_gruppo, start=1):
            data = [
                idx, fattura.nome_file, truncate_string(fattura.cessionario_denominazione) + f" (P.IVA/C.F.: {fattura.cessionario_id_fiscale})",
                fattura.numero, fattura.data.strftime('%d/%m/%Y') if fattura.stato_elaborazione == "OK" else "-",
                "SI" if fattura.ritenuta_applicata else "NO",
                f"{fattura.importo_ritenuta:.2f}" if fattura.ritenuta_applicata else "-",
                fattura.stato_elaborazione
            ]
            for value, width in zip(data, col_widths):
                pdf.cell(width, 10, str(value), border=1, align="C")
            pdf.ln()

    pdf.output(pdf_file_path)

if __name__ == "__main__":
    print("XML Fatture Processor 1.0")
    print("Sviluppato da Salvatore Crapanzano")
    print("Rilasciato sotto licenza GNU-GPL")
    print()

    if len(sys.argv) < 2:
        print("Sintassi corretta: python script.py /path/to/fatture [-R] [start_date end_date]")
        print("Esempio: python script.py /path/to/fatture -R 01/01/2023 31/12/2023")
        sys.exit(1)

    folder_path = sys.argv[1]
    filter_option = '-R' in sys.argv
    start_date_str = None
    end_date_str = None

    # Rimuovi l'opzione -R dagli argomenti
    argv = [arg for arg in sys.argv if arg != '-R']

    if filter_option:
        if len(argv) < 4:
            print("Sintassi corretta: python script.py /path/to/fatture -R [start_date end_date]")
            print("Esempio: python script.py /path/to/fatture -R 01/01/2023 31/12/2023")
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
        print("Sintassi corretta: python script.py /path/to/fatture [-R] [start_date end_date]")
        print("Esempio: python script.py /path/to/fatture -R 01/01/2023 31/12/2023")
        print("Formato delle date: DD/MM/YYYY")
        sys.exit(1)

    fatture = read_fatture(folder_path)

    if filter_option:
        filtered_fatture = filter_fatture_by_date_and_ritenuta(fatture, start_date, end_date)
    else:
        filtered_fatture = fatture

    pdf_file_path = "fatture.pdf"
    export_to_pdf(filtered_fatture, pdf_file_path)
    print(f"File PDF generato: {pdf_file_path}")

    aggregato = aggregate_by_supplier_and_client(filtered_fatture)
    print("\nRiepilogo Totale Ritenute per Fornitore e Cliente:")
    for fornitore, fatture_gruppo in aggregato.items():
        totale = sum(f.importo_ritenuta for f in fatture_gruppo if f.ritenuta_applicata)
        print(f"Fornitore: {fornitore}, Totale Ritenute: {totale:.2f} Euro")
