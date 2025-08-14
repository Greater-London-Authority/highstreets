#!/usr/bin/env python3
"""
Enterprise Confluence Documentation Sync Script

Features:
- Sync markdown documentation to Confluence
- Convert markdown to Confluence storage format
- Support for frontmatter-based configuration
- Dry run mode for testing
- Images handled manually in Confluence
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

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConfluenceAPI:
    """Confluence REST API client for documentation sync."""

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
        soup = BeautifulSoup(html, 'html.parser')

        # Apply transformations
        self._convert_code_blocks(soup)
        self._convert_tables(soup)
        self._convert_info_boxes(soup)
        self._process_images(soup)

        return str(soup)

    def _process_images(self, soup: BeautifulSoup):
        """Convert local image references to placeholder text."""
        for img in soup.find_all('img'):
            src = img.get('src', '')
            alt = img.get('alt', 'Image')

            # Skip external images
            if src.startswith(('http://', 'https://')):
                continue

            # Replace local images with info note
            info_box = soup.new_tag('div')
            info_box['class'] = 'confluence-information-macro'

            info_icon = soup.new_tag('span')
            info_icon['class'] = 'aui-icon aui-icon-small aui-iconfont-info'
            info_icon.string = 'Info'

            info_body = soup.new_tag('div')
            info_body['class'] = 'confluence-information-macro-body'

            info_text = soup.new_tag('p')
            info_text.string = f"📊 {alt} - Please add image manually in Confluence"

            info_body.append(info_text)
            info_box.append(info_icon)
            info_box.append(info_body)

            img.replace_with(info_box)
            logger.info(f"Converted image placeholder: {alt}")

    def _convert_code_blocks(self, soup: BeautifulSoup):
        """Convert code blocks to Confluence code macros."""
        from bs4 import CData

        for pre in soup.find_all('pre'):
            code = pre.find('code')
            if code:
                classes = code.get('class', [])
                language = self._extract_language(classes)

                macro = soup.new_tag('ac:structured-macro')
                macro['ac:name'] = 'code'

                if language:
                    lang_param = soup.new_tag('ac:parameter')
                    lang_param['ac:name'] = 'language'
                    lang_param.string = language
                    macro.append(lang_param)

                body = soup.new_tag('ac:plain-text-body')
                # Fix: Properly wrap code content in CDATA for Confluence
                code_content = code.get_text().strip()
                body.append(CData(code_content))
                macro.append(body)

                pre.replace_with(macro)

    def _convert_tables(self, soup: BeautifulSoup):
        """Enhanced table conversion for Confluence."""
        for table in soup.find_all('table'):
            table['class'] = 'confluenceTable'

            for th in table.find_all('th'):
                th['class'] = 'confluenceTh'

            for td in table.find_all('td'):
                td['class'] = 'confluenceTd'

    def _convert_info_boxes(self, soup: BeautifulSoup):
        """Convert blockquotes to Confluence info macros."""
        for blockquote in soup.find_all('blockquote'):
            content = blockquote.get_text().strip()

            if (content.startswith('**Important**:') or  # noqa
                    content.startswith('**⚠️')):
                macro_type = 'warning'
            elif (content.startswith('**Note**:') or  # noqa
                    content.startswith('**📝')):
                macro_type = 'info'
            elif (content.startswith('**Success**:') or  # noqa
                    content.startswith('**✅')):
                macro_type = 'note'
            else:
                continue

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
    """Sync orchestrator for documentation."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.confluence = self._init_confluence_api()
        self.processor = MarkdownProcessor()
        self.stats = {
            'success_count': 0,
            'error_count': 0,
            'total_pages': 0,
            'new_pages': 0,
            'updated_pages': 0
        }

    def _init_confluence_api(self) -> ConfluenceAPI:
        """Initialize Confluence API client."""
        required_vars = ['CONFLUENCE_URL', 'CONFLUENCE_USERNAME',
                         'CONFLUENCE_API_TOKEN']
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: "
                f"{', '.join(missing_vars)}")

        return ConfluenceAPI(
            base_url=os.getenv('CONFLUENCE_URL'),
            username=os.getenv('CONFLUENCE_USERNAME'),
            api_token=os.getenv('CONFLUENCE_API_TOKEN')
        )

    def sync_documentation(self, confluence_dir: str = 'confluence',
                           space_key: str = None):
        """Sync all documentation files to Confluence."""
        if not space_key:
            space_key = os.getenv('CONFLUENCE_SPACE', 'CDU')

        # Verify space exists
        space = self.confluence.get_space(space_key)
        if not space:
            logger.error(f"Space {space_key} not found or not accessible")
            return False

        logger.info(f"Syncing documentation to Confluence space: {space_key}")

        # Find all markdown files
        md_files = list(Path(confluence_dir).rglob('*.md'))
        # Skip README files
        md_files = [f for f in md_files if f.name != 'README.md']

        self.stats['total_pages'] = len(md_files)

        # Process files
        for md_file in md_files:
            try:
                self._sync_file(md_file, space_key)
                self.stats['success_count'] += 1
            except Exception as e:
                logger.error(f"Failed to sync {md_file}: {e}")
                self.stats['error_count'] += 1

        self._save_sync_summary()
        logger.info(
            f"Sync completed: {self.stats['success_count']} successful, "
            f"{self.stats['error_count']} errors")
        return self.stats['error_count'] == 0

    def _sync_file(self, md_file: Path, space_key: str):
        """Sync individual markdown file."""
        logger.info(f"Processing: {md_file}")

        # Parse frontmatter
        with open(md_file, 'r', encoding='utf-8') as f:
            post = frontmatter.load(f)

        title = post.metadata.get('title', md_file.stem)
        parent_title = post.metadata.get('parent')

        if self.dry_run:
            logger.info(f"[DRY RUN] Would sync: {title}")
            return

        # Convert markdown to Confluence format
        content = self.processor.convert_to_confluence_storage(post.content)

        # Find existing page
        existing_page = self.confluence.get_page_by_title(space_key, title)

        if existing_page:
            # Update existing page
            result = self.confluence.update_page(
                existing_page['id'],
                title,
                content,
                existing_page['version']['number']
            )
            if result:
                self.stats['updated_pages'] += 1
                logger.info(f"Updated page: {title}")
        else:
            # Create new page
            parent_id = None
            if parent_title:
                parent_page = self.confluence.get_page_by_title(
                    space_key, parent_title)
                if parent_page:
                    parent_id = parent_page['id']

            result = self.confluence.create_page(
                space_key, title, content, parent_id)
            if result:
                self.stats['new_pages'] += 1
                logger.info(f"Created page: {title}")

    def _save_sync_summary(self):
        """Save sync summary for GitHub Actions."""
        summary = {
            'success_count': self.stats['success_count'],
            'error_count': self.stats['error_count'],
            'total_pages': self.stats['total_pages'],
            'new_pages': self.stats['new_pages'],
            'updated_pages': self.stats['updated_pages'],
            'space_name': os.getenv('CONFLUENCE_SPACE', 'CDU'),
            'space_url': (f"{os.getenv('CONFLUENCE_URL')}/spaces/"
                          f"{os.getenv('CONFLUENCE_SPACE', 'CDU')}")
        }

        # Use OS-appropriate temp directory
        temp_dir = os.environ.get('TEMP', '/tmp')
        summary_file = os.path.join(temp_dir,
                                    'sync_summary.json' if not self.dry_run
                                    else 'dry_run_summary.json')
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description='Sync documentation to Confluence')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run without making changes')
    parser.add_argument('--space', help='Confluence space key')
    parser.add_argument('--dir', default='confluence',
                        help='Documentation directory')

    args = parser.parse_args()

    try:
        sync = ConfluenceSync(dry_run=args.dry_run)
        success = sync.sync_documentation(args.dir, args.space)
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
