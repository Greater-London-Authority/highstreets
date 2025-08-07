#!/usr/bin/env python3
"""
Enhanced Enterprise Confluence Documentation Sync Script with Image Upload

Features:
- Automatic image upload to Confluence as attachments
- Attachment token fallback for better permissions handling
- Image reference conversion in markdown
- Support for local image files
- All existing functionality preserved
"""

import os
import sys
import json
import argparse
import logging
import re
import mimetypes
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
    """Enhanced Confluence REST API client with attachment token fallback."""
    
    def __init__(self, base_url: str, username: str, api_token: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.api_token = api_token
        
        # Primary session for general operations
        self.session = requests.Session()
        self.session.auth = (username, api_token)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Attachment-specific session (try fallback token if available)
        self.attachment_session = None
        attachment_token = os.getenv('CONFLUENCE_ATTACHMENT_TOKEN')
        if attachment_token and attachment_token != api_token:
            self.attachment_session = requests.Session()
            self.attachment_session.auth = (username, attachment_token)
            logger.info("Using dedicated attachment token for file uploads")
    
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

    def upload_attachment(self, page_id: str, file_path: str, 
                         filename: str) -> Optional[str]:
        """Upload file as attachment with token fallback."""
        sessions_to_try = []
        
        # Try attachment-specific session first if available
        if self.attachment_session:
            sessions_to_try.append(("attachment token", 
                                   self.attachment_session))
        
        # Fallback to main session
        sessions_to_try.append(("main token", self.session))
        
        for token_type, session in sessions_to_try:
            try:
                logger.info(f"Attempting upload with {token_type}: {filename}")
                
                # Always create new attachment (simpler and more reliable)
                url = f"{self.base_url}/rest/api/content/{page_id}/child/attachment"
                
                # Determine content type
                content_type, _ = mimetypes.guess_type(file_path)
                if not content_type:
                    content_type = 'application/octet-stream'
                
                # Upload file
                with open(file_path, 'rb') as f:
                    files = {'file': (filename, f, content_type)}
                    headers = {key: val for key, val in session.headers.items() 
                              if key.lower() != 'content-type'}
                    
                    response = requests.post(url, files=files, 
                                           auth=session.auth, headers=headers)
                    response.raise_for_status()
                    
                    result = response.json()
                    if 'results' in result:
                        result = result['results'][0]
                    
                    logger.info(f"Successfully uploaded with {token_type}: {filename}")
                    return filename
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    logger.warning(f"403 Forbidden with {token_type} for {filename}")
                    continue  # Try next token
                else:
                    logger.error(f"HTTP error with {token_type} for {filename}: {e}")
                    continue
            except Exception as e:
                logger.error(f"Upload failed with {token_type} for {filename}: {e}")
                continue
        
        logger.error(f"All token attempts failed for {filename}")
        return None

    def get_page_attachments(self, page_id: str) -> List[Dict]:
        """Get all attachments for a page."""
        try:
            url = f"{self.base_url}/rest/api/content/{page_id}/child/attachment"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json().get('results', [])
        except Exception as e:
            logger.error(f"Failed to get attachments for page {page_id}: {e}")
            return []


class MarkdownProcessor:
    """Enhanced markdown processor with image upload support."""
    
    def __init__(self, confluence_api: ConfluenceAPI):
        self.confluence_api = confluence_api
        self.md = markdown.Markdown(extensions=[
            'markdown.extensions.tables',
            'markdown.extensions.fenced_code',
            'markdown.extensions.codehilite',
            'markdown.extensions.toc'
        ])
    
    def process_images_in_markdown(self, content: str, page_id: str, 
                                 source_file_path: str) -> str:
        """Process markdown images and upload them as Confluence attachments."""
        # Find all image references
        img_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
        images = re.findall(img_pattern, content)
        
        source_dir = os.path.dirname(source_file_path)
        
        for alt_text, img_path in images:
            # Skip external URLs
            if img_path.startswith(('http://', 'https://')):
                continue
                
            # Handle relative paths
            if not img_path.startswith('/'):
                # Relative to the markdown file
                full_img_path = os.path.join(source_dir, img_path)
            else:
                # Absolute path from repo root
                full_img_path = img_path.lstrip('/')
            
            # Normalize path
            full_img_path = os.path.normpath(full_img_path)
            
            if os.path.exists(full_img_path):
                filename = os.path.basename(full_img_path)
                logger.info(f"Processing image: {filename} from {full_img_path}")
                
                attachment_filename = self.confluence_api.upload_attachment(
                    page_id, full_img_path, filename)
                
                if attachment_filename:
                    # Replace markdown image with Confluence image macro
                    old_img = f'![{alt_text}]({img_path})'
                    new_img = (f'<ac:image ac:width="800">'
                             f'<ri:attachment ri:filename="{attachment_filename}" />'
                             f'</ac:image>')
                    content = content.replace(old_img, new_img)
                    logger.info(f"Converted image: {filename}")
                else:
                    logger.warning(f"Failed to upload image: {filename}")
            else:
                logger.warning(f"Image file not found: {full_img_path}")
        
        return content
    
    def convert_to_confluence_storage(self, markdown_content: str, 
                                    page_id: str = None, 
                                    source_file_path: str = None) -> str:
        """Convert markdown to Confluence storage format with image processing."""
        # Process images first if we have page context
        if page_id and source_file_path:
            markdown_content = self.process_images_in_markdown(
                markdown_content, page_id, source_file_path)
        
        # Convert markdown to HTML
        html = self.md.convert(markdown_content)
        soup = BeautifulSoup(html, 'html.parser')
        
        # Apply existing transformations
        self._convert_code_blocks(soup)
        self._convert_tables(soup)
        self._convert_info_boxes(soup)
        
        return str(soup)
    
    def _convert_code_blocks(self, soup: BeautifulSoup):
        """Convert code blocks to Confluence code macros."""
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
                body.string = code.get_text()
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
    """Enhanced sync orchestrator with image support."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.confluence = self._init_confluence_api()
        self.processor = MarkdownProcessor(self.confluence)
        self.stats = {
            'success_count': 0,
            'error_count': 0,
            'total_pages': 0,
            'new_pages': 0,
            'updated_pages': 0,
            'images_uploaded': 0
        }
    
    def _init_confluence_api(self) -> ConfluenceAPI:
        """Initialize Confluence API client."""
        required_vars = ['CONFLUENCE_URL', 'CONFLUENCE_USERNAME', 
                        'CONFLUENCE_API_TOKEN']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(
                f"Missing required environment variables for Confluence API: "
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
        
        # Find existing page
        existing_page = self.confluence.get_page_by_title(space_key, title)
        
        if existing_page:
            # Update existing page with images
            content = self.processor.convert_to_confluence_storage(
                post.content, 
                existing_page['id'], 
                str(md_file)
            )
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
            # Create new page (two-step process for images)
            initial_content = self.processor.convert_to_confluence_storage(
                post.content)
            
            parent_id = None
            if parent_title:
                parent_page = self.confluence.get_page_by_title(
                    space_key, parent_title)
                if parent_page:
                    parent_id = parent_page['id']
            
            result = self.confluence.create_page(
                space_key, title, initial_content, parent_id)
            if result:
                self.stats['new_pages'] += 1
                
                # Now process images for the newly created page
                final_content = self.processor.convert_to_confluence_storage(
                    post.content, 
                    result['id'], 
                    str(md_file)
                )
                
                # Update page with images
                self.confluence.update_page(
                    result['id'], 
                    title, 
                    final_content, 
                    result['version']['number']
                )
                
                logger.info(f"Created page: {title}")
    
    def _save_sync_summary(self):
        """Save sync summary for GitHub Actions."""
        summary = {
            'success_count': self.stats['success_count'],
            'error_count': self.stats['error_count'],
            'total_pages': self.stats['total_pages'],
            'new_pages': self.stats['new_pages'],
            'updated_pages': self.stats['updated_pages'],
            'images_uploaded': self.stats['images_uploaded'],
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