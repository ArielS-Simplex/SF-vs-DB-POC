#!/usr/bin/env python3
"""
Nuvei DWH Platform POC - ETL Asset Extraction Framework
Discovers, catalogs, and analyzes existing Databricks ETL scripts and utilities
"""

import os
import sys
import json
import logging
import ast
import re
from pathlib import Path
from typing import Dict, List, Set, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from shared.utilities.config_manager import config_manager
from shared.utilities.connection_manager import connection_manager


@dataclass
class ETLAsset:
    """Data class representing a discovered ETL asset"""
    file_path: str
    asset_type: str  # 'notebook', 'script', 'sql', 'config'
    name: str
    size_bytes: int
    last_modified: str
    language: str  # 'python', 'sql', 'scala', 'r'
    complexity_score: int  # 1-10 scale
    dependencies: List[str]
    database_objects: List[str]  # tables, views referenced
    transformations: List[str]  # types of data transformations
    business_priority: str  # based on acquired company context
    migration_difficulty: str  # 'easy', 'medium', 'hard', 'critical'
    description: str
    extracted_sql: List[str]  # SQL statements found
    spark_operations: List[str]  # Spark-specific operations
    data_sources: List[str]  # Input data sources
    data_targets: List[str]  # Output targets


@dataclass
class ETLInventory:
    """Complete inventory of discovered ETL assets"""
    total_assets: int
    assets_by_type: Dict[str, int]
    assets_by_language: Dict[str, int]
    assets_by_priority: Dict[str, int]
    total_complexity_score: int
    critical_dependencies: List[str]
    migration_summary: Dict[str, int]
    discovered_assets: List[ETLAsset]
    extraction_timestamp: str


class ETLAssetExtractor:
    """Main class for extracting and analyzing ETL assets"""
    
    def __init__(self):
        self.console = Console()
        self.logger = logging.getLogger(__name__)
        self.extraction_results = ETLInventory(
            total_assets=0,
            assets_by_type={},
            assets_by_language={},
            assets_by_priority={},
            total_complexity_score=0,
            critical_dependencies=[],
            migration_summary={},
            discovered_assets=[],
            extraction_timestamp=datetime.now().isoformat()
        )
        
        # Business priority mapping based on acquired companies
        self.priority_keywords = {
            'paymentez': 'CRITICAL',  # 65 employees, unknown stack
            'mazooma': 'HIGH_RISK',   # 6 employees, DB2 legacy
            'simplex': 'HIGH',        # 61 employees, already Snowflake
            'safecharge': 'HIGH',     # Large team, SQL Server
            'default': 'MEDIUM'
        }
        
        # Common Databricks/Spark patterns
        self.spark_patterns = [
            r'spark\.',
            r'\.sql\(',
            r'\.createOrReplaceTempView\(',
            r'\.write\.',
            r'\.read\.',
            r'DataFrame',
            r'pyspark',
            r'from pyspark',
            r'spark\.sql',
            r'\.cache\(\)',
            r'\.persist\(\)',
            r'\.join\(',
            r'\.groupBy\(',
            r'\.agg\(',
            r'\.select\(',
            r'\.filter\(',
            r'\.where\(',
        ]
        
        # SQL patterns for extraction
        self.sql_patterns = [
            r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?(?:VIEW|TABLE)',
            r'INSERT\s+(?:INTO|OVERWRITE)',
            r'SELECT\s+.+?\s+FROM',
            r'UPDATE\s+.+?\s+SET',
            r'DELETE\s+FROM',
            r'WITH\s+\w+\s+AS',
            r'MERGE\s+INTO',
        ]


    def discover_asset_locations(self) -> List[Path]:
        """Discover potential locations of ETL assets"""
        search_paths = []
        
        # Standard Databricks locations
        databricks_paths = [
            project_root / 'databricks' / 'original_scripts',
            project_root / 'databricks' / 'schemas',
            project_root / 'databricks' / 'sample_data',
        ]
        
        # User workspace paths (common locations)
        user_home = Path.home()
        potential_paths = [
            user_home / 'Databricks',
            user_home / 'databricks-cli',
            user_home / '.databricks',
            user_home / 'Documents' / 'Databricks',
            user_home / 'PycharmProjects',
            user_home / 'repos',
            user_home / 'workspace',
        ]
        
        # Add existing paths
        for path in databricks_paths:
            if path.exists():
                search_paths.append(path)
        
        # Add user paths that exist
        for path in potential_paths:
            if path.exists():
                search_paths.append(path)
        
        # Add current working directory
        search_paths.append(Path.cwd())
        
        return search_paths


    def extract_file_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract basic metadata from a file"""
        try:
            stat = file_path.stat()
            return {
                'size_bytes': stat.st_size,
                'last_modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'extension': file_path.suffix.lower(),
                'name': file_path.stem
            }
        except Exception as e:
            self.logger.warning(f"Could not extract metadata from {file_path}: {e}")
            return {
                'size_bytes': 0,
                'last_modified': datetime.now().isoformat(),
                'extension': '',
                'name': str(file_path.name)
            }


    def determine_language(self, file_path: Path, content: str) -> str:
        """Determine the programming language of a file"""
        extension = file_path.suffix.lower()
        
        # Extension-based detection
        extension_map = {
            '.py': 'python',
            '.sql': 'sql',
            '.scala': 'scala',
            '.r': 'r',
            '.ipynb': 'notebook',
            '.json': 'config',
            '.yaml': 'config',
            '.yml': 'config',
        }
        
        if extension in extension_map:
            return extension_map[extension]
        
        # Content-based detection
        if any(pattern in content.lower() for pattern in ['import pyspark', 'from pyspark', 'spark.sql']):
            return 'python'
        elif any(pattern in content.upper() for pattern in ['SELECT', 'CREATE TABLE', 'INSERT INTO']):
            return 'sql'
        elif 'import org.apache.spark' in content:
            return 'scala'
        
        return 'unknown'


    def calculate_complexity_score(self, content: str, language: str) -> int:
        """Calculate complexity score (1-10) based on content analysis"""
        score = 1
        
        # Base complexity factors
        lines = content.split('\n')
        line_count = len([l for l in lines if l.strip()])
        
        # Line count factor
        if line_count > 500:
            score += 3
        elif line_count > 200:
            score += 2
        elif line_count > 50:
            score += 1
        
        # Language-specific complexity
        if language == 'python':
            # Python complexity patterns
            if len(re.findall(r'def\s+\w+', content)) > 10:
                score += 2
            if len(re.findall(r'class\s+\w+', content)) > 3:
                score += 2
            if 'lambda' in content:
                score += 1
                
        elif language == 'sql':
            # SQL complexity patterns
            if len(re.findall(r'JOIN', content.upper())) > 5:
                score += 2
            if len(re.findall(r'CASE\s+WHEN', content.upper())) > 3:
                score += 2
            if 'RECURSIVE' in content.upper():
                score += 3
        
        # Spark-specific complexity
        spark_operations = sum(1 for pattern in self.spark_patterns 
                             if re.search(pattern, content, re.IGNORECASE))
        if spark_operations > 10:
            score += 3
        elif spark_operations > 5:
            score += 2
        elif spark_operations > 0:
            score += 1
        
        return min(score, 10)  # Cap at 10


    def extract_dependencies(self, content: str, language: str) -> List[str]:
        """Extract dependencies from file content"""
        dependencies = []
        
        if language == 'python':
            # Python imports
            import_patterns = [
                r'import\s+(\w+(?:\.\w+)*)',
                r'from\s+(\w+(?:\.\w+)*)\s+import',
            ]
            
            for pattern in import_patterns:
                matches = re.findall(pattern, content)
                dependencies.extend(matches)
        
        # Database/table references
        table_patterns = [
            r'FROM\s+(\w+(?:\.\w+)*)',
            r'INTO\s+(\w+(?:\.\w+)*)',
            r'TABLE\s+(\w+(?:\.\w+)*)',
            r'VIEW\s+(\w+(?:\.\w+)*)',
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            dependencies.extend(matches)
        
        return list(set(dependencies))  # Remove duplicates


    def extract_database_objects(self, content: str) -> List[str]:
        """Extract database objects (tables, views) referenced in content"""
        objects = []
        
        # Common SQL patterns for database objects
        patterns = [
            r'FROM\s+([`"]?\w+(?:\.\w+)*[`"]?)',
            r'JOIN\s+([`"]?\w+(?:\.\w+)*[`"]?)',
            r'INTO\s+([`"]?\w+(?:\.\w+)*[`"]?)',
            r'UPDATE\s+([`"]?\w+(?:\.\w+)*[`"]?)',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+([`"]?\w+(?:\.\w+)*[`"]?)',
            r'INSERT\s+(?:INTO|OVERWRITE)\s+([`"]?\w+(?:\.\w+)*[`"]?)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            objects.extend([match.strip('`"') for match in matches])
        
        return list(set(objects))


    def extract_transformations(self, content: str) -> List[str]:
        """Extract types of data transformations from content"""
        transformations = []
        
        # Common transformation patterns
        transform_patterns = {
            'aggregation': [r'GROUP\s+BY', r'SUM\(', r'COUNT\(', r'AVG\(', r'MAX\(', r'MIN\(', r'\.agg\('],
            'join': [r'JOIN', r'\.join\('],
            'filter': [r'WHERE', r'HAVING', r'\.filter\(', r'\.where\('],
            'window': [r'OVER\s*\(', r'ROW_NUMBER\(\)', r'RANK\(\)', r'DENSE_RANK\(\)'],
            'pivot': [r'PIVOT', r'UNPIVOT', r'\.pivot\('],
            'union': [r'UNION', r'\.union\(', r'\.unionAll\('],
            'distinct': [r'DISTINCT', r'\.distinct\('],
            'sort': [r'ORDER\s+BY', r'\.sort\(', r'\.orderBy\('],
        }
        
        for transform_type, patterns in transform_patterns.items():
            if any(re.search(pattern, content, re.IGNORECASE) for pattern in patterns):
                transformations.append(transform_type)
        
        return transformations


    def determine_business_priority(self, file_path: str, content: str) -> str:
        """Determine business priority based on file path and content"""
        file_path_lower = file_path.lower()
        content_lower = content.lower()
        
        # Check for company-specific keywords
        for company, priority in self.priority_keywords.items():
            if company in file_path_lower or company in content_lower:
                return priority
        
        return self.priority_keywords['default']


    def determine_migration_difficulty(self, asset: ETLAsset) -> str:
        """Determine migration difficulty based on asset characteristics"""
        # Start with complexity score
        if asset.complexity_score >= 8:
            difficulty = 'critical'
        elif asset.complexity_score >= 6:
            difficulty = 'hard'
        elif asset.complexity_score >= 4:
            difficulty = 'medium'
        else:
            difficulty = 'easy'
        
        # Adjust based on language and operations
        if asset.language == 'scala':
            difficulty = 'hard'  # Scala to Snowflake is complex
        elif asset.language == 'sql' and not asset.spark_operations:
            difficulty = 'easy'  # Pure SQL is easier to migrate
        
        # Adjust based on business priority
        if asset.business_priority in ['CRITICAL', 'HIGH_RISK']:
            if difficulty == 'easy':
                difficulty = 'medium'  # Even easy migrations are riskier for critical systems
        
        return difficulty


    def extract_sql_statements(self, content: str) -> List[str]:
        """Extract SQL statements from content"""
        sql_statements = []
        
        for pattern in self.sql_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
            sql_statements.extend(matches)
        
        return sql_statements


    def extract_spark_operations(self, content: str) -> List[str]:
        """Extract Spark-specific operations from content"""
        operations = []
        
        for pattern in self.spark_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            operations.extend(matches)
        
        return list(set(operations))


    def analyze_single_file(self, file_path: Path) -> Optional[ETLAsset]:
        """Analyze a single file and create ETL asset"""
        try:
            # Read file content
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract metadata
            metadata = self.extract_file_metadata(file_path)
            
            # Determine asset type
            if file_path.suffix.lower() == '.ipynb':
                asset_type = 'notebook'
            elif file_path.suffix.lower() == '.sql':
                asset_type = 'sql'
            elif file_path.suffix.lower() in ['.json', '.yaml', '.yml']:
                asset_type = 'config'
            else:
                asset_type = 'script'
            
            # Analyze content
            language = self.determine_language(file_path, content)
            complexity_score = self.calculate_complexity_score(content, language)
            dependencies = self.extract_dependencies(content, language)
            database_objects = self.extract_database_objects(content)
            transformations = self.extract_transformations(content)
            business_priority = self.determine_business_priority(str(file_path), content)
            extracted_sql = self.extract_sql_statements(content)
            spark_operations = self.extract_spark_operations(content)
            
            # Create asset
            asset = ETLAsset(
                file_path=str(file_path),
                asset_type=asset_type,
                name=metadata['name'],
                size_bytes=metadata['size_bytes'],
                last_modified=metadata['last_modified'],
                language=language,
                complexity_score=complexity_score,
                dependencies=dependencies,
                database_objects=database_objects,
                transformations=transformations,
                business_priority=business_priority,
                migration_difficulty='',  # Will be set later
                description=f"{asset_type.title()} containing {len(transformations)} transformation types",
                extracted_sql=extracted_sql,
                spark_operations=spark_operations,
                data_sources=[],  # TODO: Enhance to detect data sources
                data_targets=[]   # TODO: Enhance to detect data targets
            )
            
            # Set migration difficulty
            asset.migration_difficulty = self.determine_migration_difficulty(asset)
            
            return asset
            
        except Exception as e:
            self.logger.warning(f"Could not analyze file {file_path}: {e}")
            return None


    def scan_directory(self, directory: Path, file_extensions: Set[str]) -> List[Path]:
        """Recursively scan directory for ETL files"""
        etl_files = []
        
        try:
            for item in directory.rglob('*'):
                if item.is_file() and item.suffix.lower() in file_extensions:
                    etl_files.append(item)
        except PermissionError:
            self.logger.warning(f"Permission denied accessing {directory}")
        except Exception as e:
            self.logger.warning(f"Error scanning {directory}: {e}")
        
        return etl_files


    def extract_assets_from_databricks_workspace(self) -> List[ETLAsset]:
        """Extract assets from connected Databricks workspace"""
        assets = []
        
        try:
            # This would connect to Databricks API to list workspace files
            # For now, we'll focus on local file discovery
            self.console.print("📡 Databricks workspace extraction not yet implemented")
            self.console.print("💡 Add Databricks CLI integration for full workspace access")
            
        except Exception as e:
            self.logger.warning(f"Could not extract from Databricks workspace: {e}")
        
        return assets


    def run_extraction(self) -> ETLInventory:
        """Run complete ETL asset extraction process"""
        self.console.print(Panel(
            "[bold cyan]Nuvei DWH Platform POC[/bold cyan]\n"
            "[dim]ETL Asset Extraction & Analysis Framework[/dim]\n\n"
            "Discovering and cataloging existing Databricks ETL assets\n"
            "for migration analysis and Snowflake refactoring",
            title="🔍 ETL Asset Discovery",
            border_style="blue"
        ))
        
        # File extensions to look for
        etl_extensions = {'.py', '.sql', '.ipynb', '.scala', '.r', '.json', '.yaml', '.yml'}
        
        # Discover search locations
        search_paths = self.discover_asset_locations()
        self.console.print(f"\n📂 Scanning {len(search_paths)} potential locations...")
        
        all_assets = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=self.console
        ) as progress:
            
            # Scan local directories
            scan_task = progress.add_task("Scanning directories...", total=len(search_paths))
            
            for search_path in search_paths:
                progress.update(scan_task, description=f"Scanning {search_path.name}...")
                
                etl_files = self.scan_directory(search_path, etl_extensions)
                
                if etl_files:
                    analyze_task = progress.add_task(
                        f"Analyzing {len(etl_files)} files in {search_path.name}...", 
                        total=len(etl_files)
                    )
                    
                    for file_path in etl_files:
                        asset = self.analyze_single_file(file_path)
                        if asset:
                            all_assets.append(asset)
                        progress.advance(analyze_task)
                    
                    progress.remove_task(analyze_task)
                
                progress.advance(scan_task)
            
            # Extract from Databricks workspace (if configured)
            workspace_task = progress.add_task("Extracting from Databricks workspace...", total=None)
            workspace_assets = self.extract_assets_from_databricks_workspace()
            all_assets.extend(workspace_assets)
            progress.remove_task(workspace_task)
        
        # Compile inventory
        self.extraction_results.discovered_assets = all_assets
        self.extraction_results.total_assets = len(all_assets)
        
        # Calculate statistics
        self._calculate_inventory_statistics()
        
        return self.extraction_results


    def _calculate_inventory_statistics(self):
        """Calculate summary statistics for the inventory"""
        assets = self.extraction_results.discovered_assets
        
        # Assets by type
        type_counts = {}
        for asset in assets:
            type_counts[asset.asset_type] = type_counts.get(asset.asset_type, 0) + 1
        self.extraction_results.assets_by_type = type_counts
        
        # Assets by language
        language_counts = {}
        for asset in assets:
            language_counts[asset.language] = language_counts.get(asset.language, 0) + 1
        self.extraction_results.assets_by_language = language_counts
        
        # Assets by priority
        priority_counts = {}
        for asset in assets:
            priority_counts[asset.business_priority] = priority_counts.get(asset.business_priority, 0) + 1
        self.extraction_results.assets_by_priority = priority_counts
        
        # Migration summary
        migration_counts = {}
        for asset in assets:
            migration_counts[asset.migration_difficulty] = migration_counts.get(asset.migration_difficulty, 0) + 1
        self.extraction_results.migration_summary = migration_counts
        
        # Total complexity
        self.extraction_results.total_complexity_score = sum(asset.complexity_score for asset in assets)
        
        # Critical dependencies
        all_deps = []
        for asset in assets:
            all_deps.extend(asset.dependencies)
        
        # Find most common dependencies (critical ones)
        dep_counts = {}
        for dep in all_deps:
            dep_counts[dep] = dep_counts.get(dep, 0) + 1
        
        # Top 10 most referenced dependencies
        sorted_deps = sorted(dep_counts.items(), key=lambda x: x[1], reverse=True)
        self.extraction_results.critical_dependencies = [dep[0] for dep in sorted_deps[:10]]


    def display_extraction_results(self):
        """Display extraction results in formatted tables"""
        results = self.extraction_results
        
        # Summary statistics table
        summary_table = Table(title="📊 ETL Asset Discovery Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Count", justify="right", style="bold")
        summary_table.add_column("Business Impact", style="dim")
        
        summary_table.add_row("Total Assets Discovered", str(results.total_assets), "Migration scope")
        summary_table.add_row("Total Complexity Score", str(results.total_complexity_score), "Development effort")
        summary_table.add_row("Critical Dependencies", str(len(results.critical_dependencies)), "Integration risk")
        
        self.console.print(summary_table)
        
        # Assets by type
        if results.assets_by_type:
            type_table = Table(title="📁 Assets by Type")
            type_table.add_column("Asset Type", style="cyan")
            type_table.add_column("Count", justify="right")
            type_table.add_column("Percentage", justify="right")
            
            for asset_type, count in results.assets_by_type.items():
                percentage = (count / results.total_assets) * 100
                type_table.add_row(asset_type.title(), str(count), f"{percentage:.1f}%")
            
            self.console.print(type_table)
        
        # Migration difficulty
        if results.migration_summary:
            migration_table = Table(title="🎯 Migration Difficulty Assessment")
            migration_table.add_column("Difficulty", style="cyan")
            migration_table.add_column("Count", justify="right")
            migration_table.add_column("Risk Level", style="dim")
            
            difficulty_map = {
                'easy': '🟢 Low Risk',
                'medium': '🟡 Medium Risk',
                'hard': '🟠 High Risk',
                'critical': '🔴 Critical Risk'
            }
            
            for difficulty, count in results.migration_summary.items():
                risk_level = difficulty_map.get(difficulty, '⚪ Unknown')
                migration_table.add_row(difficulty.title(), str(count), risk_level)
            
            self.console.print(migration_table)
        
        # Business priority
        if results.assets_by_priority:
            priority_table = Table(title="🏢 Assets by Business Priority")
            priority_table.add_column("Priority", style="cyan")
            priority_table.add_column("Count", justify="right")
            priority_table.add_column("Acquired Company Context", style="dim")
            
            priority_context = {
                'CRITICAL': 'Paymentez (65 employees, unknown stack)',
                'HIGH_RISK': 'Mazooma (6 employees, DB2 legacy)',
                'HIGH': 'SafeCharge, Simplex (large teams)',
                'MEDIUM': 'Other acquired companies'
            }
            
            for priority, count in results.assets_by_priority.items():
                context = priority_context.get(priority, 'Standard migration')
                priority_table.add_row(priority, str(count), context)
            
            self.console.print(priority_table)


    def save_inventory_report(self, output_path: Optional[Path] = None) -> Path:
        """Save complete inventory report to JSON file"""
        if output_path is None:
            output_path = project_root / 'comparison' / 'results' / 'etl_asset_inventory.json'
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to dictionary for JSON serialization
        inventory_dict = asdict(self.extraction_results)
        
        # Save to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(inventory_dict, f, indent=2, ensure_ascii=False)
        
        self.console.print(f"\n💾 Inventory report saved to: {output_path}")
        return output_path


def main():
    """Main function for ETL asset extraction"""
    console = Console()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(project_root / 'logs' / 'etl_extraction.log', mode='w'),
            logging.StreamHandler()
        ]
    )
    
    # Ensure required directories exist
    (project_root / 'logs').mkdir(exist_ok=True)
    (project_root / 'comparison' / 'results').mkdir(parents=True, exist_ok=True)
    
    # Run extraction
    extractor = ETLAssetExtractor()
    
    try:
        # Extract assets
        inventory = extractor.run_extraction()
        
        # Display results
        console.print("\n" + "="*80)
        extractor.display_extraction_results()
        
        # Save report
        report_path = extractor.save_inventory_report()
        
        # Executive summary
        console.print(Panel(
            f"[bold green]✅ ETL Asset Extraction Complete[/bold green]\n\n"
            f"📊 Discovered: {inventory.total_assets} ETL assets\n"
            f"🎯 Complexity Score: {inventory.total_complexity_score}\n"
            f"📁 Report Location: {report_path}\n\n"
            f"[bold]Next Steps:[/bold]\n"
            f"1. Review discovered assets for migration priority\n"
            f"2. Begin Snowflake schema creation\n"
            f"3. Start refactoring critical assets first\n"
            f"4. Set up automated validation framework",
            title="🏢 Nuvei Executive Summary",
            border_style="green"
        ))
        
        return True
        
    except Exception as e:
        console.print(f"\n[bold red]❌ ETL extraction failed: {e}[/bold red]")
        logging.error(f"ETL extraction failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
