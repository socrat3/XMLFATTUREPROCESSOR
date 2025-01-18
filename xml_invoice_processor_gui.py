import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from tkcalendar import DateEntry
from datetime import datetime
import subprocess
import os
import urllib.request

class InvoiceProcessorGUI:
    def __init__(self, root):
        # Adattare la GUI alle dimensioni dello schermo
        self.root = root
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        self.root.title("Elaboratore XML Fatture")
        self.root.geometry(f"{screen_width}x{screen_height}")

        # Collegare i tasti funzione
        self.root.bind("<F5>", lambda event: self.process_invoices())
        self.root.bind("<F3>", lambda event: self.open_pdf())
        self.root.bind("<F10>", lambda event: self.root.quit())

        # Creare il frame principale con padding
        main_frame = ttk.Frame(root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        # Sezione intestazione con informazioni su versione e licenza
        ttk.Label(main_frame, text="Elaboratore XML Fatture - Versione 1.1 del 18/01/2025", font=("Helvetica", 12, "bold")).grid(row=0, column=0, columnspan=3, pady=10)
        ttk.Label(main_frame, text="Autore: Salvatore Crapanzano - Piccola Agrigento", font=("Helvetica", 10)).grid(row=1, column=0, columnspan=3, pady=5)
        ttk.Label(main_frame, text="Licenza: GNU GPL", font=("Helvetica", 10, "italic")).grid(row=2, column=0, columnspan=3, pady=5)

        # Selezione della cartella per le fatture
        ttk.Label(main_frame, text="Seleziona Cartella Fatture:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.folder_path = tk.StringVar()
        self.folder_entry = ttk.Entry(main_frame, textvariable=self.folder_path, width=50)
        self.folder_entry.grid(row=3, column=1, padx=5, pady=5)
        ttk.Button(main_frame, text="Sfoglia", command=self.browse_folder).grid(row=3, column=2, pady=5)

        # Opzioni
        ttk.Label(main_frame, text="Opzioni:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.option_r = tk.BooleanVar(value=False)
        self.option_m = tk.BooleanVar(value=False)
        ttk.Checkbutton(main_frame, text="-R (filtra ritenute)", variable=self.option_r).grid(row=4, column=1, sticky=tk.W, padx=5)
        ttk.Checkbutton(main_frame, text="-M (salva riepilogo)", variable=self.option_m).grid(row=4, column=2, sticky=tk.W, padx=5)

        # Input per Partita IVA del fornitore
        ttk.Label(main_frame, text="Partita IVA Fornitore:").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.supplier_vat = tk.StringVar()
        self.supplier_vat_entry = ttk.Entry(main_frame, textvariable=self.supplier_vat, width=50)
        self.supplier_vat_entry.grid(row=5, column=1, padx=5, pady=5)

        # Input per Partita IVA del cliente
        ttk.Label(main_frame, text="Partita IVA Cliente:").grid(row=6, column=0, sticky=tk.W, pady=5)
        self.client_vat = tk.StringVar()
        self.client_vat_entry = ttk.Entry(main_frame, textvariable=self.client_vat, width=50)
        self.client_vat_entry.grid(row=6, column=1, padx=5, pady=5)

        # Input per la data di inizio
        ttk.Label(main_frame, text="Data Inizio:").grid(row=7, column=0, sticky=tk.W, pady=5)
        self.start_date = DateEntry(main_frame, width=20, background='darkblue',
                                    foreground='white', borderwidth=2, date_pattern='dd/MM/yyyy')
        self.start_date.set_date(datetime(2024, 1, 1))  # Impostare la data predefinita
        self.start_date.grid(row=7, column=1, sticky=tk.W, padx=5, pady=5)

        # Input per la data di fine
        ttk.Label(main_frame, text="Data Fine:").grid(row=8, column=0, sticky=tk.W, pady=5)
        self.end_date = DateEntry(main_frame, width=20, background='darkblue',
                                  foreground='white', borderwidth=2, date_pattern='dd/MM/yyyy')
        self.end_date.set_date(datetime(2024, 12, 31))  # Impostare la data predefinita
        self.end_date.grid(row=8, column=1, sticky=tk.W, padx=5, pady=5)

        # Pulsanti centrali
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=9, column=0, columnspan=3, pady=20)

        self.process_button = ttk.Button(button_frame, text="Elabora Fatture (F5)", command=self.process_invoices)
        self.process_button.grid(row=0, column=0, padx=10)

        self.open_pdf_button = ttk.Button(button_frame, text="Apri PDF (F3)", command=self.open_pdf, state='disabled')
        self.open_pdf_button.grid(row=0, column=1, padx=10)

        self.clear_button = ttk.Button(button_frame, text="Reinizializza Campi", command=self.clear_fields)
        self.clear_button.grid(row=0, column=2, padx=10)

        self.exit_button = ttk.Button(button_frame, text="Esci (F10)", command=self.root.quit)
        self.exit_button.grid(row=0, column=3, padx=10)

        # Frame con scrollbar per l'output del prompt
        output_frame = ttk.Frame(main_frame, borderwidth=1, relief="solid")
        output_frame.grid(row=10, column=0, columnspan=3, pady=10, sticky=(tk.W, tk.E, tk.N, tk.S))
        main_frame.rowconfigure(10, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.columnconfigure(2, weight=1)

        # Scrollbar verticale e orizzontale
        y_scroll = ttk.Scrollbar(output_frame, orient=tk.VERTICAL)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        x_scroll = ttk.Scrollbar(output_frame, orient=tk.HORIZONTAL)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        # Controllo Text per l'output
        self.output_text = tk.Text(output_frame, wrap=tk.NONE, yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set, state=tk.DISABLED)
        self.output_text.pack(expand=True, fill=tk.BOTH)
        y_scroll.config(command=self.output_text.yview)
        x_scroll.config(command=self.output_text.xview)

        # Variabile per il percorso del file PDF generato
        self.pdf_path = None

        # Controllo dell'esistenza del file processore
        self.check_processor_file()

    def browse_folder(self):
        folder_selected = filedialog.askdirectory()
        self.folder_path.set(folder_selected)

    def check_processor_file(self):
        processor_path = "xml_fatture_processor.py"
        if not os.path.exists(processor_path):
            self.log_output("File 'xml_fatture_processor.py' non trovato. Scaricamento in corso...\n", "orange")
            try:
                url = "https://raw.githubusercontent.com/socrat3/XMLFATTUREPROCESSOR/main/xml_fatture_processor.py"
                urllib.request.urlretrieve(url, processor_path)
                self.log_output("File 'xml_fatture_processor.py' scaricato con successo!\n", "green")
            except Exception as e:
                self.log_output(f"Errore durante il download: {str(e)}\n", "red")

    def process_invoices(self):
        folder = self.folder_path.get()
        start_date = self.start_date.get_date().strftime('%d/%m/%Y')
        end_date = self.end_date.get_date().strftime('%d/%m/%Y')
        supplier_vat = self.supplier_vat.get()
        client_vat = self.client_vat.get()
        option_r = "-R" if self.option_r.get() else ""
        option_m = "-M" if self.option_m.get() else ""

        if not folder:
            self.log_output("Seleziona prima una cartella!\n", "red")
            return

        command = ["python", "xml_fatture_processor.py", folder, option_r, option_m]
        if supplier_vat:
            command += ["-FORNITORE", supplier_vat]
        if client_vat:
            command += ["-CLIENTE", client_vat]
        command += [start_date, end_date]

        self.log_output(f"Eseguendo comando: {' '.join(command)}\n", "blue")
        self.process_button.config(state='disabled')
        self.root.after(100, lambda: self.run_processing(command))

    def run_processing(self, command):
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            self.log_output(result.stdout, "green")
            for line in result.stdout.splitlines():
                if line.startswith("File PDF generato:"):
                    self.pdf_path = line.split(":", 1)[1].strip()
                    self.open_pdf_button.config(state='normal')
        except subprocess.CalledProcessError as e:
            self.log_output(e.stderr, "red")
        except Exception as e:
            self.log_output(f"Errore: {str(e)}\n", "red")
        finally:
            self.process_button.config(state='normal')

    def log_output(self, message, color):
        self.output_text.config(state=tk.NORMAL)
        self.output_text.insert(tk.END, message)
        self.output_text.config(state=tk.DISABLED)

    def open_pdf(self):
        if self.pdf_path and os.path.exists(self.pdf_path):
            try:
                os.startfile(self.pdf_path)
            except Exception as e:
                self.log_output(f"Errore durante l'apertura del PDF: {str(e)}\n", "red")
        else:
            self.log_output("File PDF non trovato!\n", "red")

    def clear_fields(self):
        self.folder_path.set("")
        self.supplier_vat.set("")
        self.client_vat.set("")
        self.start_date.set_date(datetime(2024, 1, 1))
        self.end_date.set_date(datetime(2024, 12, 31))
        self.option_r.set(False)
        self.option_m.set(False)
        self.output_text.config(state=tk.NORMAL)
        self.output_text.delete(1.0, tk.END)
        self.output_text.config(state=tk.DISABLED)
        self.open_pdf_button.config(state='disabled')

def main():
    root = tk.Tk()
    app = InvoiceProcessorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
