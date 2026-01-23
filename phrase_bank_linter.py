#!/usr/bin/env python3
"""
PHRASE_BANK Constitutional Linter
Validates phrases against Language Constitution rules
"""

import csv
import re
from collections import defaultdict, Counter

# Constitutional violations
BANNED_WORDS = {
    'price': ['cheap', 'expensive', 'value', 'deal', 'affordable', 'budget', 'cost', 'price', 'markup'],
    'superlative': ['best', 'perfect', 'ideal', 'ultimate', 'top', 'finest', 'greatest'],
    'marketing': ['amazing', 'incredible', 'wonderful', 'fantastic', 'spectacular', 'breathtaking', 'stunning'],
    'urgency': ['must-see', "don't miss", "can't miss", 'hurry', 'limited', 'now', 'today'],
    'discovery': ['discover', 'explore', 'experience', 'find', 'uncover'],
    'authenticity': ['authentic', 'genuine', 'real', 'true', 'actual'],
    'gems': ['hidden gem', 'off the beaten', 'secret', 'locals only'],
    'comparative': ['without', 'instead of', 'better than', 'rather than', 'vs', 'versus'],
    'defensive': ['surprisingly', 'actually', 'really', 'truly', 'quite'],
    'paradise': ['paradise', 'heaven', 'dream', 'magical', 'enchanting']
}

CONTEXT_HINTS = {'nature', 'culture', 'food', 'outdoors', 'urban', 'coastal', 'quiet', 'active'}

MAX_PHRASES_PER_COMBO = 4
MAX_PHRASE_LENGTH = 60

class PhraseLinter:
    def __init__(self):
        self.violations = []
        self.warnings = []
        self.stats = {
            'total_phrases': 0,
            'overages': 0,
            'violations': 0,
            'warnings': 0
        }
        self.phrases_per_combo = defaultdict(int)
    
    def check_banned_words(self, phrase, dest, theme):
        """Check for constitutionally banned words"""
        phrase_lower = phrase.lower()
        found = []
        
        for category, words in BANNED_WORDS.items():
            for word in words:
                if word in phrase_lower:
                    found.append((category, word))
        
        return found
    
    def check_compound_claims(self, phrase):
        """Check for multiple claims in one phrase (weak signal)"""
        # Look for "and" connecting two independent concepts
        if ' and ' in phrase.lower():
            parts = phrase.lower().split(' and ')
            if len(parts) == 2:
                # Simple heuristic: if both parts are >3 words, might be compound
                if len(parts[0].split()) > 3 and len(parts[1].split()) > 3:
                    return True
        return False
    
    def check_exclamation(self, phrase):
        """Check for exclamation points (sales punctuation)"""
        return '!' in phrase
    
    def check_length(self, phrase):
        """Check phrase length"""
        return len(phrase) > MAX_PHRASE_LENGTH
    
    def check_context_hint(self, context_hint):
        """Validate context hint"""
        if not context_hint:
            return False
        return context_hint.lower() in CONTEXT_HINTS
    
    def lint_phrase(self, row):
        """Run all checks on a single phrase"""
        phrase = row.get('phrase', '')
        dest = row.get('destination_iata', '')
        theme = row.get('theme', '')
        context_hint = row.get('context_hint', '')
        
        combo_key = (dest, theme)
        self.phrases_per_combo[combo_key] += 1
        self.stats['total_phrases'] += 1
        
        # VIOLATIONS (hard failures)
        banned = self.check_banned_words(phrase, dest, theme)
        if banned:
            for category, word in banned:
                self.violations.append({
                    'dest': dest,
                    'theme': theme,
                    'phrase': phrase,
                    'violation': f'BANNED_WORD:{category}',
                    'detail': word
                })
                self.stats['violations'] += 1
        
        if self.check_exclamation(phrase):
            self.violations.append({
                'dest': dest,
                'theme': theme,
                'phrase': phrase,
                'violation': 'EXCLAMATION_POINT',
                'detail': 'Sales punctuation not allowed'
            })
            self.stats['violations'] += 1
        
        # WARNINGS (review recommended)
        if self.check_compound_claims(phrase):
            self.warnings.append({
                'dest': dest,
                'theme': theme,
                'phrase': phrase,
                'warning': 'COMPOUND_CLAIM',
                'detail': 'Phrase may contain multiple claims'
            })
            self.stats['warnings'] += 1
        
        if self.check_length(phrase):
            self.warnings.append({
                'dest': dest,
                'theme': theme,
                'phrase': phrase,
                'warning': 'LENGTH_EXCEEDED',
                'detail': f'{len(phrase)} chars (max: {MAX_PHRASE_LENGTH})'
            })
            self.stats['warnings'] += 1
        
        if not self.check_context_hint(context_hint):
            self.warnings.append({
                'dest': dest,
                'theme': theme,
                'phrase': phrase,
                'warning': 'MISSING_CONTEXT_HINT',
                'detail': f'Invalid or missing context_hint: "{context_hint}"'
            })
            self.stats['warnings'] += 1
    
    def check_overages(self):
        """Check for destination-theme combinations exceeding 4 phrases"""
        for combo, count in self.phrases_per_combo.items():
            if count > MAX_PHRASES_PER_COMBO:
                dest, theme = combo
                self.violations.append({
                    'dest': dest,
                    'theme': theme,
                    'phrase': '[OVERAGE]',
                    'violation': 'PHRASE_DENSITY_EXCEEDED',
                    'detail': f'{count} phrases (max: {MAX_PHRASES_PER_COMBO})'
                })
                self.stats['overages'] += 1
    
    def generate_report(self):
        """Generate lint report"""
        report = []
        report.append("=" * 80)
        report.append("PHRASE_BANK CONSTITUTIONAL LINT REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary stats
        report.append("📊 SUMMARY")
        report.append(f"   Total phrases: {self.stats['total_phrases']}")
        report.append(f"   Total combinations: {len(self.phrases_per_combo)}")
        report.append(f"   Violations: {self.stats['violations']}")
        report.append(f"   Warnings: {self.stats['warnings']}")
        report.append(f"   Overages: {self.stats['overages']}")
        report.append("")
        
        # Violations
        if self.violations:
            report.append("🔴 VIOLATIONS (Constitutional Breaches)")
            report.append("-" * 80)
            
            # Group by violation type
            by_type = defaultdict(list)
            for v in self.violations:
                by_type[v['violation']].append(v)
            
            for vtype, items in sorted(by_type.items()):
                report.append(f"\n{vtype}: {len(items)} occurrences")
                for item in items[:5]:  # Show first 5 of each type
                    if item['phrase'] == '[OVERAGE]':
                        report.append(f"   {item['dest']} / {item['theme']}: {item['detail']}")
                    else:
                        report.append(f"   {item['dest']} / {item['theme']}")
                        report.append(f"      \"{item['phrase']}\"")
                        report.append(f"      Issue: {item['detail']}")
                if len(items) > 5:
                    report.append(f"   ... and {len(items) - 5} more")
            report.append("")
        
        # Warnings
        if self.warnings:
            report.append("🟡 WARNINGS (Review Recommended)")
            report.append("-" * 80)
            
            by_type = defaultdict(list)
            for w in self.warnings:
                by_type[w['warning']].append(w)
            
            for wtype, items in sorted(by_type.items()):
                report.append(f"\n{wtype}: {len(items)} occurrences")
                for item in items[:3]:  # Show first 3 of each type
                    report.append(f"   {item['dest']} / {item['theme']}")
                    report.append(f"      \"{item['phrase']}\"")
                    report.append(f"      Note: {item['detail']}")
                if len(items) > 3:
                    report.append(f"   ... and {len(items) - 3} more")
            report.append("")
        
        # Clean bill of health
        if not self.violations and not self.warnings:
            report.append("✅ CLEAN")
            report.append("   No violations or warnings found.")
            report.append("   Phrase bank is constitutionally compliant.")
            report.append("")
        
        report.append("=" * 80)
        report.append("END REPORT")
        report.append("=" * 80)
        
        return "\n".join(report)


def lint_phrase_bank(filepath):
    """Main linting function"""
    linter = PhraseLinter()
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            linter.lint_phrase(row)
    
    linter.check_overages()
    return linter.generate_report()


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python phrase_bank_linter.py <path_to_phrase_bank.csv>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    report = lint_phrase_bank(filepath)
    print(report)
    
    # Optionally save report
    with open('phrase_bank_lint_report.txt', 'w') as f:
        f.write(report)
    print("\nReport saved to: phrase_bank_lint_report.txt")
