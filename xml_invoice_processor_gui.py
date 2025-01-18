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
        self.root = root
        self.root.title("Elaboratore XML Fatture")
        self.root.geometry("600x500")

        # Create main frame with padding
        main_frame = ttk.Frame(root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Folder Selection
        ttk.Label(main_frame, text="Seleziona Cartella Fatture:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.folder_path = tk.StringVar()
        self.folder_entry = ttk.Entry(main_frame, textvariable=self.folder_path, width=50)
        self.folder_entry.grid(row=0, column=1, padx=5, pady=5)
        ttk.Button(main_frame, text="Sfoglia", command=self.browse_folder).grid(row=0, column=2, pady=5)

        # Options
        ttk.Label(main_frame, text="Opzioni:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.option_r = tk.BooleanVar(value=False)
        self.option_m = tk.BooleanVar(value=False)
        ttk.Checkbutton(main_frame, text="-R (filtra ritenute)", variable=self.option_r).grid(row=1, column=1, sticky=tk.W, padx=5)
        ttk.Checkbutton(main_frame, text="-M (salva riepilogo)", variable=self.option_m).grid(row=1, column=2, sticky=tk.W, padx=5)

        # Supplier VAT
        ttk.Label(main_frame, text="Partita IVA Fornitore:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.supplier_vat = tk.StringVar()
        self.supplier_vat_entry = ttk.Entry(main_frame, textvariable=self.supplier_vat, width=50)
        self.supplier_vat_entry.grid(row=2, column=1, padx=5, pady=5)

        # Client VAT
        ttk.Label(main_frame, text="Partita IVA Cliente:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.client_vat = tk.StringVar()
        self.client_vat_entry = ttk.Entry(main_frame, textvariable=self.client_vat, width=50)
        self.client_vat_entry.grid(row=3, column=1, padx=5, pady=5)

        # Start Date
        ttk.Label(main_frame, text="Data Inizio:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.start_date = DateEntry(main_frame, width=20, background='darkblue',
                                    foreground='white', borderwidth=2, date_pattern='dd/MM/yyyy')
        self.start_date.grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)

        # End Date
        ttk.Label(main_frame, text="Data Fine:").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.end_date = DateEntry(main_frame, width=20, background='darkblue',
                                  foreground='white', borderwidth=2, date_pattern='dd/MM/yyyy')
        self.end_date.grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)

        # Process Button
        self.process_button = ttk.Button(main_frame, text="Elabora Fatture",
                                          command=self.process_invoices)
        self.process_button.grid(row=6, column=0, columnspan=3, pady=20)

        # Open PDF Button
        self.open_pdf_button = ttk.Button(main_frame, text="Apri PDF", command=self.open_pdf, state='disabled')
        self.open_pdf_button.grid(row=7, column=0, columnspan=3, pady=10)

        # Status Label
        self.status_label = ttk.Label(main_frame, text="")
        self.status_label.grid(row=8, column=0, columnspan=3, pady=5)

        # Progress Bar
        self.progress = ttk.Progressbar(main_frame, length=400, mode='indeterminate')
        self.progress.grid(row=9, column=0, columnspan=3, pady=5)

        # PDF Path
        self.pdf_path = None

        # Check if the processor file exists
        self.check_processor_file()

    def browse_folder(self):
        folder_selected = filedialog.askdirectory()
        self.folder_path.set(folder_selected)

    def check_processor_file(self):
        processor_path = "xml_fatture_processor.py"
        if not os.path.exists(processor_path):
            self.status_label.config(
                text="File 'xml_fatture_processor.py' non trovato. Scaricamento in corso...",
                foreground="orange"
            )
            try:
                url = "https://raw.githubusercontent.com/socrat3/XMLFATTUREPROCESSOR/main/xml_fatture_processor.py"
                urllib.request.urlretrieve(url, processor_path)
                self.status_label.config(
                    text="File 'xml_fatture_processor.py' scaricato con successo!",
                    foreground="green"
                )
            except Exception as e:
                self.status_label.config(
                    text=f"Errore durante il download: {str(e)}", foreground="red"
                )

    def process_invoices(self):
        # Get values
        folder = self.folder_path.get()
        start_date = self.start_date.get_date().strftime('%d/%m/%Y')
        end_date = self.end_date.get_date().strftime('%d/%m/%Y')
        supplier_vat = self.supplier_vat.get()
        client_vat = self.client_vat.get()
        option_r = "-R" if self.option_r.get() else ""
        option_m = "-M" if self.option_m.get() else ""

        if not folder:
            self.status_label.config(text="Seleziona prima una cartella!", foreground="red")
            return

        # Construct command
        command = ["python", "xml_fatture_processor.py", folder, option_r, option_m]
        if supplier_vat:
            command += ["-FORNITORE", supplier_vat]
        if client_vat:
            command += ["-CLIENTE", client_vat]
        command += [start_date, end_date]

        # Update UI
        self.status_label.config(text="Elaborazione in corso...", foreground="blue")
        self.process_button.config(state='disabled')
        self.progress.start()

        # Execute the command
        self.root.after(100, lambda: self.run_processing(command))

    def run_processing(self, command):
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            self.status_label.config(
                text="Elaborazione completata!", foreground="green"
            )
            # Parse the output to find the PDF path
            for line in result.stdout.splitlines():
                if line.startswith("File PDF generato:"):
                    self.pdf_path = line.split(":", 1)[1].strip()
                    self.open_pdf_button.config(state='normal')
        except subprocess.CalledProcessError as e:
            self.status_label.config(
                text=f"Errore: {e.stderr}", foreground="red"
            )
        except Exception as e:
            self.status_label.config(
                text=f"Errore durante l'elaborazione: {str(e)}", foreground="red"
            )
        finally:
            self.progress.stop()
            self.process_button.config(state='normal')

    def open_pdf(self):
        if self.pdf_path and os.path.exists(self.pdf_path):
            try:
                os.startfile(self.pdf_path)
            except Exception as e:
                self.status_label.config(
                    text=f"Errore durante l'apertura del PDF: {str(e)}", foreground="red"
                )
        else:
            self.status_label.config(
                text="File PDF non trovato!", foreground="red"
            )

def main():
    root = tk.Tk()
    app = InvoiceProcessorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
