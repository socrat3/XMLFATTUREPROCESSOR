#!/usr/bin/env python3
"""
SISTEMA DIAGNOSTICO AVANZATO - Analisi Completa Elaborazione Fatture
Diagnostica dettagliata per identificare discrepanze nei file elaborati
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import re

@dataclass
class FileProcessingStatus:
    """Stato completo di elaborazione di un singolo file."""
    filename: str
    original_path: str
    file_type: str  # P7M, XML, UNKNOWN
    file_size: int
    
    # Fase Download
    downloaded: bool = False
    download_method: str = "NONE"
    
    # Fase Decodifica
    needs_decoding: bool = False
    decoding_attempted: bool = False
    decoding_successful: bool = False
    decoding_method: str = "NONE"
    decoding_errors: List[str] = None
    decoded_size: int = 0
    
    # Fase Parsing
    parsing_attempted: bool = False
    parsing_successful: bool = False
    parsing_errors: List[str] = None
    identified_type: str = "UNKNOWN"  # INVOICE, METADATA, NOTIFICATION
    belongs_to_portfolio: bool = False
    
    # Fase Organizzazione
    organization_attempted: bool = False
    organization_successful: bool = False
    organization_errors: List[str] = None
    final_path: str = ""
    
    # Duplicati
    is_duplicate: bool = False
    duplicate_of: str = ""
    
    # Timestamp
    processed_at: str = ""

    def __post_init__(self):
        if self.decoding_errors is None:
            self.decoding_errors = []
        if self.parsing_errors is None:
            self.parsing_errors = []
        if self.organization_errors is None:
            self.organization_errors = []
        if self.processed_at == "":
            self.processed_at = datetime.now().isoformat()

@dataclass
class ProcessingDiagnostics:
    """Diagnostica completa del processo di elaborazione."""
    total_files: int
    
    # Breakdown per fase
    download_stats: Dict[str, int]
    decoding_stats: Dict[str, int] 
    parsing_stats: Dict[str, int]
    organization_stats: Dict[str, int]
    
    # File problematici
    failed_files: List[FileProcessingStatus]
    duplicate_files: List[FileProcessingStatus]
    unrecognized_files: List[FileProcessingStatus]
    
    # Dettaglio errori
    decoding_failures: List[Tuple[str, List[str]]]
    parsing_failures: List[Tuple[str, List[str]]]
    organization_failures: List[Tuple[str, List[str]]]
    
    # Portfolio analysis
    portfolio_matches: int
    non_portfolio_files: int
    
    # Raccomandazioni
    recommendations: List[str]
    
    # Timestamp
    generated_at: str = ""

    def __post_init__(self):
        if self.generated_at == "":
            self.generated_at = datetime.now().isoformat()

class AdvancedDiagnosticSystem:
    """Sistema diagnostico avanzato per l'elaborazione delle fatture."""
    
    def __init__(self, config: Dict, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.file_statuses: Dict[str, FileProcessingStatus] = {}
        self.processing_phases = ['download', 'decoding', 'parsing', 'organization']
        
    def register_file(self, filepath: Path, file_type: str = None) -> FileProcessingStatus:
        """Registra un nuovo file nel sistema diagnostico."""
        file_key = str(filepath)
        
        if file_key not in self.file_statuses:
            # Determina tipo file se non specificato
            if file_type is None:
                if filepath.suffix.lower() == '.p7m':
                    file_type = 'P7M'
                elif filepath.suffix.lower() == '.xml':
                    file_type = 'XML'
                else:
                    file_type = 'UNKNOWN'
            
            # Determina se necessita decodifica
            needs_decoding = file_type == 'P7M'
            
            status = FileProcessingStatus(
                filename=filepath.name,
                original_path=str(filepath),
                file_type=file_type,
                file_size=filepath.stat().st_size if filepath.exists() else 0,
                needs_decoding=needs_decoding
            )
            
            self.file_statuses[file_key] = status
            self.logger.debug(f"Registrato file: {filepath.name} (tipo: {file_type})")
        
        return self.file_statuses[file_key]
    
    def update_download_status(self, filepath: Path, success: bool, method: str = "ADE_API"):
        """Aggiorna stato download."""
        status = self.register_file(filepath)
        status.downloaded = success
        status.download_method = method
        
    def update_decoding_status(self, filepath: Path, attempted: bool, success: bool, 
                             method: str = "NONE", errors: List[str] = None, 
                             decoded_size: int = 0):
        """Aggiorna stato decodifica."""
        file_key = str(filepath)
        if file_key in self.file_statuses:
            status = self.file_statuses[file_key]
            status.decoding_attempted = attempted
            status.decoding_successful = success
            status.decoding_method = method
            status.decoded_size = decoded_size
            if errors:
                status.decoding_errors.extend(errors)
    
    def update_parsing_status(self, filepath: Path, attempted: bool, success: bool,
                            identified_type: str = "UNKNOWN", belongs_to_portfolio: bool = False,
                            errors: List[str] = None):
        """Aggiorna stato parsing."""
        file_key = str(filepath)
        if file_key in self.file_statuses:
            status = self.file_statuses[file_key]
            status.parsing_attempted = attempted
            status.parsing_successful = success
            status.identified_type = identified_type
            status.belongs_to_portfolio = belongs_to_portfolio
            if errors:
                status.parsing_errors.extend(errors)
    
    def update_organization_status(self, filepath: Path, attempted: bool, success: bool,
                                 final_path: str = "", errors: List[str] = None):
        """Aggiorna stato organizzazione."""
        file_key = str(filepath)
        if file_key in self.file_statuses:
            status = self.file_statuses[file_key]
            status.organization_attempted = attempted
            status.organization_successful = success
            status.final_path = final_path
            if errors:
                status.organization_errors.extend(errors)
    
    def mark_as_duplicate(self, filepath: Path, duplicate_of: str = ""):
        """Marca file come duplicato."""
        file_key = str(filepath)
        if file_key in self.file_statuses:
            status = self.file_statuses[file_key]
            status.is_duplicate = True
            status.duplicate_of = duplicate_of
    
    def analyze_processing_pipeline(self) -> ProcessingDiagnostics:
        """Analizza l'intera pipeline di elaborazione."""
        
        # Contatori base
        total_files = len(self.file_statuses)
        
        # Statistiche per fase
        download_stats = self._calculate_phase_stats('download')
        decoding_stats = self._calculate_phase_stats('decoding')
        parsing_stats = self._calculate_phase_stats('parsing')
        organization_stats = self._calculate_phase_stats('organization')
        
        # File problematici
        failed_files = self._identify_failed_files()
        duplicate_files = [s for s in self.file_statuses.values() if s.is_duplicate]
        unrecognized_files = [s for s in self.file_statuses.values() if s.identified_type == "UNKNOWN"]
        
        # Dettaglio errori
        decoding_failures = [(s.filename, s.decoding_errors) for s in self.file_statuses.values() 
                           if s.decoding_attempted and not s.decoding_successful and s.decoding_errors]
        parsing_failures = [(s.filename, s.parsing_errors) for s in self.file_statuses.values() 
                          if s.parsing_attempted and not s.parsing_successful and s.parsing_errors]
        organization_failures = [(s.filename, s.organization_errors) for s in self.file_statuses.values() 
                               if s.organization_attempted and not s.organization_successful and s.organization_errors]
        
        # Portfolio analysis
        portfolio_matches = len([s for s in self.file_statuses.values() if s.belongs_to_portfolio])
        non_portfolio_files = len([s for s in self.file_statuses.values() 
                                 if s.parsing_successful and not s.belongs_to_portfolio])
        
        # Genera raccomandazioni
        recommendations = self._generate_recommendations()
        
        return ProcessingDiagnostics(
            total_files=total_files,
            download_stats=download_stats,
            decoding_stats=decoding_stats,
            parsing_stats=parsing_stats,
            organization_stats=organization_stats,
            failed_files=failed_files,
            duplicate_files=duplicate_files,
            unrecognized_files=unrecognized_files,
            decoding_failures=decoding_failures,
            parsing_failures=parsing_failures,
            organization_failures=organization_failures,
            portfolio_matches=portfolio_matches,
            non_portfolio_files=non_portfolio_files,
            recommendations=recommendations
        )
    
    def _calculate_phase_stats(self, phase: str) -> Dict[str, int]:
        """Calcola statistiche per una fase specifica."""
        stats = {
            'total': 0,
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
        for status in self.file_statuses.values():
            if phase == 'download':
                stats['total'] += 1
                if status.downloaded:
                    stats['attempted'] += 1
                    stats['successful'] += 1
                else:
                    stats['failed'] += 1
                    
            elif phase == 'decoding':
                if status.needs_decoding:
                    stats['total'] += 1
                    if status.decoding_attempted:
                        stats['attempted'] += 1
                        if status.decoding_successful:
                            stats['successful'] += 1
                        else:
                            stats['failed'] += 1
                    else:
                        stats['skipped'] += 1
                        
            elif phase == 'parsing':
                stats['total'] += 1
                if status.parsing_attempted:
                    stats['attempted'] += 1
                    if status.parsing_successful:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                else:
                    stats['skipped'] += 1
                    
            elif phase == 'organization':
                stats['total'] += 1
                if status.organization_attempted:
                    stats['attempted'] += 1
                    if status.organization_successful:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                else:
                    stats['skipped'] += 1
        
        return stats
    
    def _identify_failed_files(self) -> List[FileProcessingStatus]:
        """Identifica file che hanno fallito in qualsiasi fase."""
        failed = []
        
        for status in self.file_statuses.values():
            if (not status.downloaded or 
                (status.needs_decoding and status.decoding_attempted and not status.decoding_successful) or
                (status.parsing_attempted and not status.parsing_successful) or
                (status.organization_attempted and not status.organization_successful)):
                failed.append(status)
        
        return failed
    
    def _generate_recommendations(self) -> List[str]:
        """Genera raccomandazioni basate sull'analisi."""
        recommendations = []
        
        # Analizza fallimenti decodifica
        decoding_failures = len([s for s in self.file_statuses.values() 
                               if s.decoding_attempted and not s.decoding_successful])
        if decoding_failures > 0:
            recommendations.append(f"🔧 {decoding_failures} file P7M falliti - Verificare integrità file e certificati")
        
        # Analizza file non del portfolio
        non_portfolio = len([s for s in self.file_statuses.values() 
                           if s.parsing_successful and not s.belongs_to_portfolio])
        if non_portfolio > 0:
            recommendations.append(f"📋 {non_portfolio} fatture non appartengono al portfolio configurato")
        
        # Analizza duplicati
        duplicates = len([s for s in self.file_statuses.values() if s.is_duplicate])
        if duplicates > 0:
            recommendations.append(f"🔄 {duplicates} file duplicati rilevati - Sistema di hash funzionante")
        
        # Analizza discrepanze organizzazione
        parsing_ok = len([s for s in self.file_statuses.values() if s.parsing_successful])
        organization_ok = len([s for s in self.file_statuses.values() if s.organization_successful])
        if parsing_ok > organization_ok:
            diff = parsing_ok - organization_ok
            recommendations.append(f"⚠️ {diff} file parsati correttamente ma non organizzati - Verificare logica organizzazione")
        
        # Verifica performance decodifica
        p7m_files = len([s for s in self.file_statuses.values() if s.file_type == 'P7M'])
        asn1_success = len([s for s in self.file_statuses.values() 
                          if s.decoding_method == 'ASN1' and s.decoding_successful])
        if p7m_files > 0 and (asn1_success / p7m_files) > 0.9:
            recommendations.append("✅ Decoder ASN1 molto efficace - Performance ottimale")
        
        return recommendations
    
    def generate_detailed_report(self, diagnostics: ProcessingDiagnostics) -> str:
        """Genera report diagnostico dettagliato."""
        
        report = [
            "=" * 80,
            "REPORT DIAGNOSTICO AVANZATO - ELABORAZIONE FATTURE",
            "=" * 80,
            f"📊 PANORAMICA GENERALE:",
            f"   File totali elaborati: {diagnostics.total_files}",
            f"   File del portfolio: {diagnostics.portfolio_matches}",
            f"   File esterni al portfolio: {diagnostics.non_portfolio_files}",
            f"   File duplicati: {len(diagnostics.duplicate_files)}",
            "",
            "🔍 ANALISI PER FASE:",
            ""
        ]
        
        # Dettaglio fasi
        phases = [
            ("DOWNLOAD", diagnostics.download_stats),
            ("DECODIFICA P7M", diagnostics.decoding_stats),
            ("PARSING XML", diagnostics.parsing_stats),
            ("ORGANIZZAZIONE", diagnostics.organization_stats)
        ]
        
        for phase_name, stats in phases:
            success_rate = (stats['successful'] / stats['total'] * 100) if stats['total'] > 0 else 0
            report.extend([
                f"📁 {phase_name}:",
                f"   Totali: {stats['total']}",
                f"   Tentati: {stats['attempted']}",
                f"   Riusciti: {stats['successful']} ({success_rate:.1f}%)",
                f"   Falliti: {stats['failed']}",
                f"   Saltati: {stats['skipped']}",
                ""
            ])
        
        # Spiegazione discrepanze numeriche
        report.extend([
            "🔍 SPIEGAZIONE DISCREPANZE NUMERICHE:",
            ""
        ])
        
        downloaded = diagnostics.download_stats['successful']
        p7m_files = len([s for s in self.file_statuses.values() if s.file_type == 'P7M'])
        xml_files = downloaded - p7m_files
        decoded = diagnostics.decoding_stats['successful']
        organized = diagnostics.organization_stats['successful']
        
        report.extend([
            f"📥 File scaricati: {downloaded}",
            f"   └─ File P7M: {p7m_files} (necessitano decodifica)",
            f"   └─ File XML: {xml_files} (già decodificati)",
            "",
            f"🔓 File P7M decodificati: {decoded}/{p7m_files}",
            f"   └─ Tasso successo: {(decoded/p7m_files*100) if p7m_files > 0 else 0:.1f}%",
            "",
            f"📁 File organizzati: {organized}",
            f"   └─ Dovrebbero essere: {decoded + xml_files} (decodificati + xml nativi)",
            f"   └─ Discrepanza: {(decoded + xml_files) - organized} file",
            ""
        ])
        
        # Dettaglio file problematici
        if diagnostics.failed_files:
            report.extend([
                "❌ FILE PROBLEMATICI:",
                ""
            ])
            for file_status in diagnostics.failed_files[:10]:  # Primi 10
                problems = []
                if not file_status.downloaded:
                    problems.append("download fallito")
                if file_status.needs_decoding and not file_status.decoding_successful:
                    problems.append("decodifica fallita")
                if file_status.parsing_attempted and not file_status.parsing_successful:
                    problems.append("parsing fallito")
                if file_status.organization_attempted and not file_status.organization_successful:
                    problems.append("organizzazione fallita")
                
                report.append(f"   🔴 {file_status.filename}: {', '.join(problems)}")
            
            if len(diagnostics.failed_files) > 10:
                report.append(f"   ... e altri {len(diagnostics.failed_files) - 10} file")
            report.append("")
        
        # Errori specifici decodifica
        if diagnostics.decoding_failures:
            report.extend([
                "🔧 ERRORI DECODIFICA P7M:",
                ""
            ])
            for filename, errors in diagnostics.decoding_failures[:5]:
                report.append(f"   🔴 {filename}:")
                for error in errors[:2]:  # Primi 2 errori
                    report.append(f"      - {error}")
            report.append("")
        
        # File non del portfolio
        if diagnostics.non_portfolio_files > 0:
            report.extend([
                f"📋 FILE NON DEL PORTFOLIO ({diagnostics.non_portfolio_files}):",
                "   Questi file sono fatture valide ma non appartengono alle aziende configurate",
                "   nel portfolio. Considerare di aggiungere le partite IVA al config.",
                ""
            ])
        
        # Raccomandazioni
        if diagnostics.recommendations:
            report.extend([
                "💡 RACCOMANDAZIONI:",
                ""
            ])
            for recommendation in diagnostics.recommendations:
                report.append(f"   {recommendation}")
            report.append("")
        
        # Riepilogo finale
        overall_success = (organized / downloaded * 100) if downloaded > 0 else 0
        report.extend([
            "📊 RIEPILOGO FINALE:",
            f"   Tasso successo generale: {overall_success:.1f}%",
            f"   File elaborati correttamente: {organized}/{downloaded}",
            f"   Timestamp: {diagnostics.generated_at}",
            "=" * 80
        ])
        
        return "\n".join(report)
    
    def save_diagnostic_data(self, output_dir: Path):
        """Salva tutti i dati diagnostici su file."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Salva stati individuali
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # File statuses
        statuses_file = output_dir / f"file_statuses_{timestamp}.json"
        with open(statuses_file, 'w', encoding='utf-8') as f:
            data = {k: asdict(v) for k, v in self.file_statuses.items()}
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        
        # Diagnostica completa
        diagnostics = self.analyze_processing_pipeline()
        diag_file = output_dir / f"diagnostics_{timestamp}.json"
        with open(diag_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(diagnostics), f, ensure_ascii=False, indent=2, default=str)
        
        # Report testuale
        report_file = output_dir / f"diagnostic_report_{timestamp}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_detailed_report(diagnostics))
        
        self.logger.info(f"Dati diagnostici salvati in: {output_dir}")
        return statuses_file, diag_file, report_file

# Funzione di utilità per integrazione
def create_diagnostic_from_log(log_content: str) -> AdvancedDiagnosticSystem:
    """Crea sistema diagnostico analizzando un log esistente."""
    # Placeholder per parsing log - implementazione specifica del formato log
    pass

def explain_numerical_discrepancies(downloaded: int, decoded: int, organized: int, 
                                  p7m_count: int = None) -> str:
    """Spiega le discrepanze numeriche in modo chiaro."""
    
    if p7m_count is None:
        p7m_count = decoded  # Stima conservativa
    
    xml_count = downloaded - p7m_count
    expected_organized = decoded + xml_count
    discrepancy = expected_organized - organized
    
    explanation = [
        f"🔍 ANALISI NUMERICA:",
        f"",
        f"📥 File scaricati: {downloaded}",
        f"   ├─ File P7M (da decodificare): {p7m_count}",
        f"   └─ File XML (già pronti): {xml_count}",
        f"",
        f"🔓 Decodifica P7M: {decoded}/{p7m_count}",
        f"   └─ Successo: {(decoded/p7m_count*100) if p7m_count > 0 else 0:.1f}%",
        f"",
        f"📁 Organizzazione:",
        f"   ├─ Previsti: {expected_organized} ({decoded} decodificati + {xml_count} XML nativi)",
        f"   ├─ Effettivi: {organized}",
        f"   └─ Discrepanza: {discrepancy} file",
        f"",
    ]
    
    if discrepancy > 0:
        explanation.extend([
            f"⚠️ CAUSE POSSIBILI DISCREPANZA:",
            f"   • File non del portfolio configurato",
            f"   • Duplicati filtrati automaticamente", 
            f"   • Errori di parsing XML",
            f"   • File corrotti o malformati",
            f"   • Ricevute SDI non processate correttamente"
        ])
    elif discrepancy < 0:
        explanation.append(f"ℹ️ File organizzati > previsti: possibili metadati aggiuntivi")
    else:
        explanation.append(f"✅ Numeri perfettamente coerenti")
    
    return "\n".join(explanation)

if __name__ == "__main__":
    # Test del sistema diagnostico
    from pathlib import Path
    import logging
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Simula il caso dal log
    print("🔍 ANALISI DEL CASO SPECIFICO DAL LOG:")
    print("=" * 50)
    
    explanation = explain_numerical_discrepancies(
        downloaded=355,
        decoded=308, 
        organized=347,
        p7m_count=309  # Dal log: "Decodifica P7M Avanzata: 309"
    )
    
    print(explanation)