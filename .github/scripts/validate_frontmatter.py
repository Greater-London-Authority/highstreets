#!/usr/bin/env python3
"""
Validate Frontmatter in Markdown Files

Ensures all markdown files have valid frontmatter configuration
for Confluence synchronization.
"""

import sys
from pathlib import Path
from typing import List, Set
import frontmatter


class FrontmatterValidator:
    """Validate frontmatter in markdown files."""

    def __init__(self):
        self.errors = []
        self.warnings = []
        self.required_fields = ['title', 'space', 'type']
        self.optional_fields = ['parent', 'labels']
        self.valid_types = ['page']
        self.valid_spaces = ['HSDS']  # Add more as needed

    def validate_directory(self, docs_dir: str = 'confluence') -> bool:
        """Validate all markdown files in directory."""
        docs_path = Path(docs_dir)

        if not docs_path.exists():
            self.errors.append(f"Documentation directory not found: {docs_dir}")
            return False

        # Find all markdown files
        markdown_files = list(docs_path.glob('*.md'))

        # Filter out README and template files
        excluded_files = ['README.md', 'data-source-template.md', 'CONFLUENCE-SETUP.md']
        markdown_files = [f for f in markdown_files
                          if f.name not in excluded_files]

        if not markdown_files:
            self.warnings.append(f"No markdown files found in {docs_dir}")
            return True

        # Validate each file
        all_valid = True
        titles_seen = set()

        for md_file in markdown_files:
            file_valid = self.validate_file(md_file, titles_seen)
            all_valid = all_valid and file_valid

        # Check for hierarchy issues
        self._validate_hierarchy(markdown_files)

        return all_valid and len(self.errors) == 0

    def validate_file(self, md_file: Path, titles_seen: Set[str]) -> bool:
        """Validate individual markdown file."""
        try:
            with open(md_file, 'r', encoding='utf-8') as f:
                post = frontmatter.load(f)
        except Exception as e:
            self.errors.append(f"{md_file.name}: Failed to parse file - {e}")
            return False

        if not post.metadata:
            self.errors.append(f"{md_file.name}: No frontmatter found")
            return False

        # Check required fields
        missing_fields = []
        for field in self.required_fields:
            if field not in post.metadata or not post.metadata[field]:
                missing_fields.append(field)

        if missing_fields:
            self.errors.append(
                f"{md_file.name}: Missing required fields: {', '.join(missing_fields)}"
            )
            return False

        # Validate field values
        file_valid = True

        # Check title uniqueness
        title = post.metadata.get('title')
        if title in titles_seen:
            self.errors.append(f"{md_file.name}: Duplicate title '{title}'")
            file_valid = False
        else:
            titles_seen.add(title)

        # Validate type
        doc_type = post.metadata.get('type')
        if doc_type not in self.valid_types:
            self.errors.append(
                f"{md_file.name}: Invalid type '{doc_type}'. "
                f"Must be one of: {', '.join(self.valid_types)}"
            )
            file_valid = False

        # Validate space
        space = post.metadata.get('space')
        if space not in self.valid_spaces:
            self.warnings.append(
                f"{md_file.name}: Space '{space}' not in known spaces: "
                f"{', '.join(self.valid_spaces)}"
            )

        # Validate labels if present
        labels = post.metadata.get('labels')
        if labels is not None:
            if not isinstance(labels, list):
                self.errors.append(
                    f"{md_file.name}: Labels must be a list, got {type(labels).__name__}"
                )
                file_valid = False
            elif any(not isinstance(label, str) for label in labels):
                self.errors.append(
                    f"{md_file.name}: All labels must be strings"
                )
                file_valid = False

        # Check for unknown fields
        all_fields = set(self.required_fields + self.optional_fields)
        unknown_fields = set(post.metadata.keys()) - all_fields
        if unknown_fields:
            self.warnings.append(
                f"{md_file.name}: Unknown frontmatter fields: "
                f"{', '.join(unknown_fields)}"
            )

        return file_valid

    def _validate_hierarchy(self, markdown_files: List[Path]):
        """Validate parent-child relationships."""
        # Build map of titles to files
        title_to_file = {}

        for md_file in markdown_files:
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    post = frontmatter.load(f)
                    title = post.metadata.get('title')
                    if title:
                        title_to_file[title] = {
                            'file': md_file,
                            'parent': post.metadata.get('parent')
                        }
            except Exception:
                continue  # Already reported in validate_file

        # Check parent references
        for title, info in title_to_file.items():
            parent_title = info['parent']
            if parent_title and parent_title not in title_to_file:
                self.warnings.append(
                    f"{info['file'].name}: Parent '{parent_title}' not found "
                    f"in current files"
                )

    def print_results(self):
        """Print validation results."""
        if self.errors:
            print("❌ Validation Errors:")
            for error in self.errors:
                print(f"  • {error}")

        if self.warnings:
            print("⚠️  Warnings:")
            for warning in self.warnings:
                print(f"  • {warning}")

        if not self.errors and not self.warnings:
            print("✅ All frontmatter is valid!")
        elif not self.errors:
            print("✅ No errors found (warnings only)")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Validate frontmatter in markdown files'
    )
    parser.add_argument('--docs-dir', default='confluence',
                        help='Documentation directory to validate')

    args = parser.parse_args()

    validator = FrontmatterValidator()
    is_valid = validator.validate_directory(args.docs_dir)

    validator.print_results()

    if not is_valid:
        sys.exit(1)
    else:
        print(f"\n🎉 Validation completed successfully for {args.docs_dir}/")


if __name__ == '__main__':
    main()
