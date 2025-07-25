#!/usr/bin/env python3
"""
Enterprise Confluence Documentation Sync Script

Syncs markdown documentation to Confluence with advanced features:
- Frontmatter-based configuration
- Hierarchical page structure
- Dry run mode
- Comprehensive error handling
- Detailed reporting
- Content validation
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional
import requests
import frontmatter
import markdown
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConfluenceAPI:
    """Confluence REST API client."""
    
    def __init__(self, base_url: str, username: str, api_token: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.auth = (username, api_token)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def get_space(self, space_key: str) -> Optional[Dict]:
        """Get space information."""
        try:
            url = f"{self.base_url}/rest/api/space/{space_key}"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get space {space_key}: {e}")
            return None
    
    def get_page_by_title(self, space_key: str, title: str) -> Optional[Dict]:
        """Find page by title in space."""
        try:
            params = {
                'spaceKey': space_key,
                'title': title,
                'expand': 'version,ancestors'
            }
            url = f"{self.base_url}/rest/api/content"
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            results = response.json().get('results', [])
            return results[0] if results else None
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to find page '{title}': {e}")
            return None
    
    def create_page(self, space_key: str, title: str, content: str, 
                    parent_id: Optional[str] = None) -> Optional[Dict]:
        """Create new page in Confluence."""
        try:
            page_data = {
                'type': 'page',
                'title': title,
                'space': {'key': space_key},
                'body': {
                    'storage': {
                        'value': content,
                        'representation': 'storage'
                    }
                }
            }
            
            if parent_id:
                page_data['ancestors'] = [{'id': parent_id}]
            
            url = f"{self.base_url}/rest/api/content"
            response = self.session.post(url, json=page_data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create page '{title}': {e}")
            return None
    
    def update_page(self, page_id: str, title: str, content: str, 
                    version: int) -> Optional[Dict]:
        """Update existing page in Confluence."""
        try:
            page_data = {
                'id': page_id,
                'type': 'page',
                'title': title,
                'body': {
                    'storage': {
                        'value': content,
                        'representation': 'storage'
                    }
                },
                'version': {
                    'number': version + 1,
                    'message': 'Updated via automated sync'
                }
            }
            
            url = f"{self.base_url}/rest/api/content/{page_id}"
            response = self.session.put(url, json=page_data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update page {page_id}: {e}")
            return None


class MarkdownProcessor:
    """Process markdown files for Confluence."""
    
    def __init__(self):
        self.md = markdown.Markdown(extensions=[
            'markdown.extensions.tables',
            'markdown.extensions.fenced_code',
            'markdown.extensions.codehilite',
            'markdown.extensions.toc'
        ])
    
    def convert_to_confluence_storage(self, markdown_content: str) -> str:
        """Convert markdown to Confluence storage format."""
        # Convert markdown to HTML
        html = self.md.convert(markdown_content)
        
        # Parse with BeautifulSoup for processing
        soup = BeautifulSoup(html, 'html.parser')
        
        # Convert code blocks to Confluence macros
        self._convert_code_blocks(soup)
        
        # Convert tables
        self._convert_tables(soup)
        
        # Convert info/warning boxes
        self._convert_info_boxes(soup)
        
        return str(soup)
    
    def _convert_code_blocks(self, soup: BeautifulSoup):
        """Convert HTML code blocks to Confluence code macros."""
        for pre in soup.find_all('pre'):
            code = pre.find('code')
            if code:
                language = self._extract_language(code.get('class', []))
                code_content = code.get_text()
                
                # Create Confluence code macro
                macro = soup.new_tag('ac:structured-macro')
                macro['ac:name'] = 'code'
                
                if language:
                    lang_param = soup.new_tag('ac:parameter')
                    lang_param['ac:name'] = 'language'
                    lang_param.string = language
                    macro.append(lang_param)
                
                body = soup.new_tag('ac:plain-text-body')
                body.string = code_content
                macro.append(body)
                
                pre.replace_with(macro)
    
    def _convert_tables(self, soup: BeautifulSoup):
        """Ensure tables are properly formatted for Confluence."""
        # Confluence usually handles HTML tables well, but we can add 
        # improvements here
        for table in soup.find_all('table'):
            table['class'] = 'confluenceTable'
    
    def _convert_info_boxes(self, soup: BeautifulSoup):
        """Convert blockquotes to Confluence info macros."""
        for blockquote in soup.find_all('blockquote'):
            # Check if it's a special info box
            content = blockquote.get_text().strip()
            
            if (content.startswith('**Important**:') or 
                    content.startswith('**⚠️')):
                macro_type = 'warning'
            elif (content.startswith('**Note**:') or 
                    content.startswith('**📝')):
                macro_type = 'info'
            elif (content.startswith('**Success**:') or 
                    content.startswith('**✅')):
                macro_type = 'note'
            else:
                continue  # Keep as regular blockquote
            
            # Create Confluence info macro
            macro = soup.new_tag('ac:structured-macro')
            macro['ac:name'] = macro_type
            
            body = soup.new_tag('ac:rich-text-body')
            body.append(soup.new_tag('p'))
            body.p.string = content
            macro.append(body)
            
            blockquote.replace_with(macro)
    
    def _extract_language(self, classes: List[str]) -> Optional[str]:
        """Extract language from code block classes."""
        for cls in classes:
            if cls.startswith('language-'):
                return cls.replace('language-', '')
            elif cls in ['python', 'bash', 'sql', 'json', 'yaml', 'javascript']:
                return cls
        return None


class ConfluenceSync:
    """Main sync orchestrator."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.confluence = self._init_confluence_api()
        self.processor = MarkdownProcessor()
        self.stats = {
            'success_count': 0,
            'error_count': 0,
            'total_pages': 0,
            'new_pages': 0,
            'updated_pages': 0,
            'errors': []
        }
    
    def _init_confluence_api(self) -> ConfluenceAPI:
        """Initialize Confluence API client."""
        from dotenv import load_dotenv
        load_dotenv()
        base_url = os.environ.get('CONFLUENCE_URL')
        username = os.environ.get('CONFLUENCE_USERNAME')
        api_token = os.environ.get('CONFLUENCE_API_TOKEN')
        
        if not all([base_url, username, api_token]):
            raise ValueError(
                "Missing required environment variables for Confluence API"
            )
        
        return ConfluenceAPI(base_url, username, api_token)
    
    def sync_documentation(self, docs_dir: str = 'confluence') -> Dict:
        """Sync all documentation files."""
        docs_path = Path(docs_dir)
        
        if not docs_path.exists():
            raise FileNotFoundError(
                f"Documentation directory not found: {docs_dir}"
            )
        
        run_type = 'dry run' if self.dry_run else 'sync'
        logger.info(f"Starting {run_type} of documentation from {docs_dir}")
        
        # Find all markdown files
        markdown_files = list(docs_path.glob('*.md'))
        
        # Filter out README and template files
        excluded_files = ['README.md', 'data-source-template.md']
        markdown_files = [f for f in markdown_files 
                          if f.name not in excluded_files]
        
        # Sort files to process parent pages first
        markdown_files = self._sort_files_by_hierarchy(markdown_files)
        
        # Process each file
        page_id_map = {}  # Track created pages for parent relationships
        
        for md_file in markdown_files:
            try:
                result = self._process_file(md_file, page_id_map)
                if result:
                    self.stats['success_count'] += 1
                    if result.get('created'):
                        self.stats['new_pages'] += 1
                    else:
                        self.stats['updated_pages'] += 1
                else:
                    self.stats['error_count'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing {md_file}: {e}")
                self.stats['error_count'] += 1
                self.stats['errors'].append(f"{md_file.name}: {str(e)}")
            
            self.stats['total_pages'] += 1
        
        # Save results
        self._save_results()
        
        success_count = self.stats['success_count']
        error_count = self.stats['error_count']
        logger.info(f"Sync completed: {success_count} successes, "
                    f"{error_count} errors")
        return self.stats
    
    def _process_file(self, md_file: Path, 
                      page_id_map: Dict[str, str]) -> Optional[Dict]:
        """Process individual markdown file."""
        logger.info(f"Processing {md_file.name}")
        
        # Parse frontmatter
        with open(md_file, 'r', encoding='utf-8') as f:
            post = frontmatter.load(f)
        
        # Extract metadata
        title = post.metadata.get('title')
        space = post.metadata.get('space', 
                                  os.environ.get('CONFLUENCE_SPACE', 'HSDS'))
        parent_title = post.metadata.get('parent')
        
        if not title:
            logger.error(f"No title found in {md_file.name}")
            return None
        
        # Convert content
        confluence_content = self.processor.convert_to_confluence_storage(
            post.content
        )
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would sync '{title}' to space '{space}'")
            return {'title': title, 'space': space, 'parent': parent_title}
        
        # Find parent page ID if specified
        parent_id = None
        if parent_title:
            parent_id = page_id_map.get(parent_title)
            if not parent_id:
                parent_page = self.confluence.get_page_by_title(
                    space, parent_title
                )
                if parent_page:
                    parent_id = parent_page['id']
                    page_id_map[parent_title] = parent_id
                else:
                    logger.warning(
                        f"Parent page '{parent_title}' not found for '{title}'"
                    )
        
        # Check if page already exists
        existing_page = self.confluence.get_page_by_title(space, title)
        
        if existing_page:
            # Update existing page
            result = self.confluence.update_page(
                existing_page['id'],
                title,
                confluence_content,
                existing_page['version']['number']
            )
            
            if result:
                logger.info(f"Updated page: {title}")
                page_id_map[title] = result['id']
                return {'title': title, 'id': result['id'], 'created': False}
            else:
                logger.error(f"Failed to update page: {title}")
                return None
        else:
            # Create new page
            result = self.confluence.create_page(
                space, title, confluence_content, parent_id
            )
            
            if result:
                logger.info(f"Created page: {title}")
                page_id_map[title] = result['id']
                return {'title': title, 'id': result['id'], 'created': True}
            else:
                logger.error(f"Failed to create page: {title}")
                return None
    
    def _sort_files_by_hierarchy(self, files: List[Path]) -> List[Path]:
        """Sort files to process parent pages before children."""
        # Simple sorting: numbered files first, then alphabetical
        def sort_key(file_path):
            name = file_path.name
            if name.startswith('01-'):
                return (0, name)  # Platform overview first
            elif name.startswith(('02-', '03-', '04-')):
                return (1, name)  # Core docs
            elif name.endswith('-data-source.md'):
                return (2, name)  # Data sources
            else:
                return (3, name)  # Everything else
        
        return sorted(files, key=sort_key)
    
    def _save_results(self):
        """Save sync results to file."""
        space = os.environ.get('CONFLUENCE_SPACE', 'HSDS')
        base_url = os.environ.get('CONFLUENCE_URL', '')
        
        results = {
            **self.stats,
            'space_name': space,
            'space_url': f"{base_url}/display/{space}",
            'dry_run': self.dry_run
        }
        
        output_file = ('/tmp/dry_run_summary.json' if self.dry_run 
                       else '/tmp/sync_summary.json')
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Sync documentation to Confluence'
    )
    parser.add_argument('--dry-run', action='store_true', 
                        help='Perform dry run without making changes')
    parser.add_argument('--docs-dir', default='confluence', 
                        help='Documentation directory')
    
    args = parser.parse_args()
    
    try:
        syncer = ConfluenceSync(dry_run=args.dry_run)
        results = syncer.sync_documentation(args.docs_dir)
        
        if results['error_count'] > 0:
            logger.error(f"Sync completed with {results['error_count']} errors")
            sys.exit(1)
        else:
            logger.info("Sync completed successfully")
            
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 