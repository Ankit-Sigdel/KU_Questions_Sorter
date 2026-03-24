#!/usr/bin/env python3
"""
KU Question Paper Sorter  v2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Reads ALL KU exam PDFs in a folder (single-course or multi-course bundles)
• Merges OCR-noisy code variants  → ONE canonical code per real course
• Extracts EVERY question (even ones that appear only once)
• Groups similar questions with NLP-style weighted similarity + stemming
• Outputs ONE PDF per course, questions sorted most-repeated → least-repeated
• Every question is printed, frequency badge shows how many times it appeared

Usage:
    python question_sorter.py                        # interactive, scans .
    python question_sorter.py /path/to/pdfs
    python question_sorter.py /path/to/pdfs --course CHEM101
    python question_sorter.py /path/to/pdfs --all    # generate all courses
"""

import os, sys, re, math, subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Set

# ── optional deps ──────────────────────────────────────────────────────────────
try:
    import pdfplumber
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.lib import colors
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        HRFlowable, PageBreak, KeepTogether, BaseDocTemplate, Frame, PageTemplate
    )
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT
    from reportlab.pdfgen import canvas as rl_canvas
except ImportError as e:
    sys.exit(f"Missing: {e}\nRun: pip install pdfplumber reportlab")

# ══════════════════════════════════════════════════════════════════════════════
#  COURSE CATALOGUE  –  canonical code → human name
#  Add new rows here whenever a new course appears.
# ══════════════════════════════════════════════════════════════════════════════
CATALOGUE: Dict[str, str] = {
    "ENGG111": "Engineering Science",
    "COMP101": "Introduction to Computing",
    "COMP102": "C Programming",
    "COMP103": "Computer Fundamentals",
    "COMP105": "Web Technology",
    "COMP115": "Database Systems",
    "COMP116": "Data Structures",
    "COMP117": "Python Programming",
    "ENVE101": "Environmental Science I",
    "ENVE102": "Environmental Engineering",
    "ENGT101": "Technical Writing I",
    "ENGT102": "Technical Writing II",
    "ENGT103": "Technical Writing III",
    "ENGT104": "Technical Communication",
    "PHYS101": "Physics I",
    "PHYS102": "Physics II",
    "PHYS104": "Physics for Technology",
    "CHEM101": "Chemistry I",
    "CHEM102": "Chemistry II",
    "MATH101": "Calculus and Linear Algebra",
    "MATH102": "Differential Equations",
    "MATH103": "Discrete Mathematics",
    "MATH104": "Numerical Methods",
    "MATH105": "Mathematics for Architecture",
    "MATH111": "Mathematics I",
    "MATH117": "Probability and Statistics",
    "MATH207": "Mathematics III",
    "BIOL101": "Biology I",
    "BIOL103": "Biology",
    "BIOT101": "Biotechnology Fundamentals",
    "ARCH101": "Building Science",
    "ARCH111": "History of Architecture I",
    "HBIO101": "Human Biology I",
    "HBIO102": "Human Biology II",
    "HBIO103": "Human Biology III",
    "STAT101": "Statistics I",
    "ENVS101": "Environmental Studies",
    "MEEG112": "Engineering Mechanics",
    "MEEG126": "Thermodynamics",
    "MEEG141": "Fluid Mechanics",
    "MEEG156": "Strength of Materials",
    "PHAR112": "Pharmacology I",
    "PHAR203": "Pharmacology III",
    "AIMA102": "AI and Machine Learning",
    "AGRS101": "Agricultural Science",
    "HIMS101": "Health Information Management I",
    "HIMS102": "Health Information Management II",
    "HIMS103": "Health Information Management III",
    "DSMA113": "Data Science Mathematics III",
    "DSMA114": "Data Science Mathematics IV",
    "DSMA115": "Data Science Mathematics V",
}

# ══════════════════════════════════════════════════════════════════════════════
#  OCR CANONICAL MAPPING
#  Maps every known noisy OCR variant → the real canonical code.
#  The function `canonicalise()` handles the systematic substitutions
#  (O→0, l→1, I→1 between digits) automatically; add ONLY edge-cases here.
# ══════════════════════════════════════════════════════════════════════════════
MANUAL_ALIASES: Dict[str, str] = {
    # OCR misreads of existing codes
    "CHEM1OL":   "CHEM101",
    "CHEMIOI":   "CHEM101",
    "CHEM101I":  "CHEM101",
    "CHEM10122": "CHEM101",
    "CHEML01":   "CHEM101",
    "CHEMI01":   "CHEM101",
    "CHEM1O1":   "CHEM101",
    "CHEM1OI":   "CHEM101",
    "CHEMIOI":   "CHEM101",
    "CHEMIO1":   "CHEM101",
    "CHEMLOL":   "CHEM101",
    "CHEML0L":   "CHEM101",
    "ENGG11I":   "ENGG111",
    "ENGGLL1":   "ENGG111",
    "ENGGL11":   "ENGG111",
    "ENGGL1I":   "ENGG111",
    "ENGGLL1":   "ENGG111",
    "COMP10I":   "COMP101",
    "COMP1OI":   "COMP101",
    "COMPL15":   "COMP115",
    "COMPL16":   "COMP116",
    "COMPI15":   "COMP115",
    "COMPI16":   "COMP116",
    "COMP1020":  "COMP102",
    "PHYS1OI":   "PHYS101",
    "PHYS10":    "PHYS101",
    "PHYS1O1":   "PHYS101",
    "MATHLL1":   "MATH111",
    "MATHL1":    "MATH111",
    "MATL1LL1":  "MATH101",   # best guess; update if wrong
    "MATHIII":   "MATH111",
    "MATH101I":  "MATH101",
    "MATH111I":  "MATH111",
    "MAL1":      "MATH101",
    "ENGCL12":   "ENGT102",
    "ENGGL12":   "ENGT102",
    "ENGGL1I":   "ENGT101",
    "ENGT10L":   "ENGT101",
    "ARCHL11":   "ARCH111",
    "ARCHLL1":   "ARCH111",
    "ARCHII":    "ARCH111",
    "BIOLL01":   "BIOL101",
    "AGBT11I":   "BIOT101",
    "AIMC101I":  "AIMC101",
    "PHARLL1":   "PHAR112",
    "PHARL12":   "PHAR112",
    "PHAR1":     "PHAR112",
    "ENVE10201": "ENVE102",
    "AIME101":   "AIMA102",
    "ACRS113":   "AGRS101",
    "MEEGL12":   "MEEG112",
    "DSMA116I5": "DSMA115",
    "COMPL1":    "COMP101",
    "COM105":    "COMP105",
    "CHEM1OL":   "CHEM101",
    "HBIO10L":   "HBIO101",
    "PHAR":      "PHAR112",
}

# ══════════════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Question:
    number:      str
    text:        str
    marks:       int
    section:     str
    source_file: str
    exam_date:   str

@dataclass
class SubPaper:
    course_code: str      # canonical
    course_name: str
    exam_date:   str
    source_file: str
    level:       str
    full_marks:  str
    questions:   List[Question] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════
#  CODE CANONICALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _raw_normalise(raw: str) -> str:
    """
    Systematic OCR fixes:
      isolated l  → 1
      I between digits → 1
      O between digits → 0
    Then strip spaces, uppercase.
    """
    s = str(raw).strip()
    # Step 1: fix l (ell) that is not inside a word like "level"
    s = re.sub(r'(?<![A-Za-z])l(?![A-Za-z])', '1', s)
    # Step 2: capital I surrounded by digits
    s = re.sub(r'(?<=\d)I(?=\d)', '1', s)
    s = re.sub(r'(?<=\s)I(?=\d)', '1', s)
    s = re.sub(r'(?<=\d)I(?=\s)', '1', s)
    # Step 3: capital O surrounded by digits
    s = re.sub(r'(?<=\d)O(?=\d)', '0', s)
    s = re.sub(r'\s+', '', s).upper()
    return s


def canonicalise(raw: str) -> str:
    """
    Turn a raw (possibly OCR-noisy) course code into the canonical form.
    Priority: manual alias table > systematic normalisation > as-is.
    """
    norm = _raw_normalise(raw)
    # Check manual aliases first
    if norm in MANUAL_ALIASES:
        return MANUAL_ALIASES[norm]
    # Check catalogue directly
    if norm in CATALOGUE:
        return norm
    # Try truncated prefix match in catalogue (e.g. CHEM10 → CHEM101)
    for cat_code in CATALOGUE:
        if norm.startswith(cat_code[:6]) or cat_code.startswith(norm[:6]):
            if abs(len(norm) - len(cat_code)) <= 2:
                return cat_code
    # Fallback: return the normalised form even if unknown
    return norm


# ══════════════════════════════════════════════════════════════════════════════
#  TEXT EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def extract_pages(pdf_path: str) -> Dict[int, str]:
    """Return {page_number: text} for every page."""
    pages: Dict[int, str] = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, 1):
                try:
                    pages[i] = page.extract_text() or ""
                except Exception:
                    pages[i] = ""
    except Exception as e:
        # fallback: pdftotext
        try:
            r = subprocess.run(
                ["pdftotext", "-layout", pdf_path, "-"],
                capture_output=True, text=True, timeout=180
            )
            if r.returncode == 0:
                for i, p in enumerate(r.stdout.split('\f'), 1):
                    pages[i] = p
        except Exception:
            pass
    return pages


# ══════════════════════════════════════════════════════════════════════════════
#  HEADER / BOUNDARY DETECTION
# ══════════════════════════════════════════════════════════════════════════════

_COURSE_RE = re.compile(r'[Cc]ourse\s*[.:]?\s*[:]\s*([A-Z]{2,6}[\s\dIlOo]{2,9})')
_DATE_RE   = re.compile(
    r'((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|'
    r'Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
    r'[/,\s\w]*\d{4})', re.IGNORECASE)
_FM_RE     = re.compile(r'F\.?\s*M\.?\s*[:=]\s*(\d+)')
_LVL_RE    = re.compile(r'[Ll]evel\s*[:]\s*([^\n]+)')
_KU_RE     = re.compile(r'KATHMANDU\s+UNIVERSITY', re.IGNORECASE)
_SEC_RE    = re.compile(r'SECTION\s+["\u201c\u201d\']?([A-E])["\u201c\u201d\']?', re.IGNORECASE)
_MARKS_RE  = re.compile(r'[\[\(](\d{1,3}(?:[+\-]\d{1,3})*(?:=\d{1,3})?)[\]\)]')


def _parse_header(text: str) -> Dict:
    out = {"course_code": "", "exam_date": "", "full_marks": "", "level": ""}
    m = _COURSE_RE.search(text)
    if m:
        out["course_code"] = canonicalise(m.group(1).strip())
    m = _DATE_RE.search(text[:800])
    if m: out["exam_date"] = m.group(1).strip()
    m = _FM_RE.search(text[:500])
    if m: out["full_marks"] = m.group(1)
    m = _LVL_RE.search(text[:400])
    if m: out["level"] = m.group(1).strip()
    return out


def _find_boundaries(pages: Dict[int, str]) -> List[Tuple[int, int, str]]:
    """Return [(start_page, end_page, canonical_code), …] for each sub-paper."""
    starts: List[Tuple[int, str]] = []
    for pnum in sorted(pages.keys()):
        if _KU_RE.search(pages[pnum]):
            m = _COURSE_RE.search(pages[pnum])
            if m:
                code = canonicalise(m.group(1))
                starts.append((pnum, code))
    if not starts:
        all_p = sorted(pages.keys())
        hdr   = _parse_header(pages[all_p[0]] if all_p else "")
        return [(all_p[0], all_p[-1], hdr["course_code"] or "UNKNOWN")]
    result = []
    for i, (start, code) in enumerate(starts):
        end = starts[i+1][0] - 1 if i+1 < len(starts) else max(pages.keys())
        result.append((start, end, code))
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  QUESTION EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def _extract_marks(text: str) -> int:
    mm = _MARKS_RE.findall(text)
    if not mm: return 0
    last = mm[-1]
    if '=' in last:
        try: return int(last.split('=')[-1])
        except: pass
    try: return sum(int(p) for p in re.split(r'[+\-]', last) if p.strip().isdigit())
    except: return 0


def extract_questions(pages: Dict[int, str], start: int, end: int,
                      source_file: str, exam_date: str) -> List[Question]:
    block = "\n".join(pages.get(p, "") for p in range(start, end + 1))
    lines = block.splitlines()
    questions: List[Question] = []
    section = "B"
    i = 0
    while i < len(lines):
        line = lines[i]
        # section change
        sm = _SEC_RE.search(line)
        if sm:
            section = sm.group(1).upper()
            i += 1
            continue
        # question start: "  1.  text" – up to 2-digit numbers, 0-6 leading spaces
        qm = re.match(r'^\s{0,6}(\d{1,2})\.\s{1,6}(\S.{2,})', line)
        if qm:
            qnum = qm.group(1)
            acc  = [line.strip()]
            j    = i + 1
            while j < len(lines):
                nxt = lines[j]
                if re.match(r'^\s{0,6}\d{1,2}\.\s{1,6}\S', nxt): break
                if _SEC_RE.search(nxt):                            break
                if re.match(r'^\s*(P\.T\.O\.?|---+|\f)',nxt,re.I):break
                s = nxt.strip()
                if s: acc.append(s)
                j += 1
            qtext = re.sub(r'\s{2,}', ' ', ' '.join(acc)).strip()
            if len(qtext) > 15 and re.search(r'[a-zA-Z]{4,}', qtext):
                questions.append(Question(
                    number=qnum, text=qtext, marks=_extract_marks(qtext),
                    section=section, source_file=source_file, exam_date=exam_date,
                ))
            i = j
            continue
        i += 1
    return questions


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESS ONE PDF
# ══════════════════════════════════════════════════════════════════════════════

def process_pdf(pdf_path: str) -> List[SubPaper]:
    fname = os.path.basename(pdf_path)
    print(f"  reading: {fname}")
    pages = extract_pages(pdf_path)
    if not pages:
        print(f"    no text extracted"); return []

    results: List[SubPaper] = []
    for start, end, code in _find_boundaries(pages):
        if code == "UNKNOWN": continue
        hdr_text = "\n".join(pages.get(p,"") for p in range(start, min(start+3,end+1)))
        hdr      = _parse_header(hdr_text)
        if not hdr["course_code"]:
            hdr["course_code"] = code
        canonical = hdr["course_code"]
        qs = extract_questions(pages, start, end, fname, hdr["exam_date"])
        results.append(SubPaper(
            course_code=canonical,
            course_name=CATALOGUE.get(canonical, ""),
            exam_date=hdr["exam_date"] or fname,
            source_file=fname,
            level=hdr["level"],
            full_marks=hdr["full_marks"],
            questions=qs,
        ))
        print(f"    {canonical:<12}  pp {start}-{end}  "
              f"{len(qs):3} Qs  [{hdr['exam_date']}]")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  NLP SIMILARITY  (improved – handles paraphrase, synonym-ish, stemming)
# ══════════════════════════════════════════════════════════════════════════════

_STOPWORDS: Set[str] = {
    'a','an','the','and','or','of','in','is','are','was','were','to','for',
    'with','that','this','by','at','from','on','be','it','its','if','as',
    'not','do','does','each','any','all','give','find','write','state',
    'explain','describe','define','discuss','what','how','why','when','where',
    'which','show','prove','derive','calculate','compute','determine','using',
    'brief','note','short','following','given','above','below','two','three',
    'also','one','can','will','has','have','had','may','must','should',
    'their','they','them','per','we','you','your','its','also','about',
    'between','difference','equation','answer','questions','examples','example',
}

# Concept synonyms: any word in a set is treated as the same concept
_SYNONYMS: List[Set[str]] = [
    {"define","definition","meaning","concept","term"},
    {"derive","derivation","deduce","deduction","obtain","proof","prove","show"},
    {"calculate","compute","find","determine","evaluate","solve"},
    {"explain","describe","discuss","elaborate","illustrate"},
    {"state","write","list","mention","give","enlist"},
    {"difference","differentiate","distinguish","compare","contrast","versus"},
    {"diagram","sketch","figure","draw","illustrate","schematic"},
    {"application","applications","use","uses","importance","advantages"},
    {"formula","equation","expression","relation","law"},
    {"temperature","thermal","heat","thermodynamic"},
    {"velocity","speed","rate","acceleration"},
    {"energy","work","power","force","momentum"},
    {"circuit","electrical","current","voltage","resistance"},
    {"organic","compound","reaction","synthesis","mechanism"},
    {"function","procedure","method","algorithm","program","code"},
    {"data","information","record","file","database"},
    {"plant","cell","organism","tissue","biology","biological"},
]

def _synonym_root(word: str) -> str:
    """Return a canonical token for synonym groups."""
    for group in _SYNONYMS:
        if word in group:
            return sorted(group)[0]   # stable canonical
    return word

def _stem(w: str) -> str:
    """Light suffix stemmer."""
    if len(w) <= 4: return w
    for suf in ("'s","s'","tion","tions","ing","ings","ment","ments",
                "ness","ical","ally","ically","ise","ize","ised","ized",
                "ous","ious","al","les","es","s","ed","er","est","ly"):
        if w.endswith(suf) and len(w)-len(suf) >= 4:
            return w[:-len(suf)]
    return w

def _tokens(text: str) -> List[str]:
    """Normalise → stem → synonym-map → filter stopwords."""
    t = text.lower()
    t = re.sub(r'[\[\(]\d[+\-x\d=]*[\]\)]', '', t)   # strip marks [6]
    t = re.sub(r'\b\d+\b', '', t)                      # strip lone numbers
    t = re.sub(r'[^\w\s]', ' ', t)
    words = [_synonym_root(_stem(w)) for w in t.split()
             if w not in _STOPWORDS and len(w) > 2]
    return words

def _tfidf_vectors(questions: List[Question]):
    """
    Build TF-IDF weight dict for each question.
    IDF is computed over the question set itself so rare domain terms
    (e.g. "bernoulli", "photosynthesis") get high weight.
    """
    docs   = [_tokens(q.text) for q in questions]
    N      = len(docs)
    # document frequency
    df: Dict[str, int] = defaultdict(int)
    for doc in docs:
        for w in set(doc):
            df[w] += 1
    # idf (add-1 smoothed)
    idf = {w: math.log((N + 1) / (cnt + 1)) for w, cnt in df.items()}
    # tf-idf vectors (L2-normalised)
    vectors = []
    for doc in docs:
        tf: Dict[str, float] = defaultdict(float)
        for w in doc: tf[w] += 1
        vec = {w: (cnt / max(len(doc),1)) * idf.get(w, 0)
               for w, cnt in tf.items()}
        norm = math.sqrt(sum(v*v for v in vec.values())) or 1.0
        vectors.append({w: v/norm for w, v in vec.items()})
    return vectors

def _cosine(va: Dict, vb: Dict) -> float:
    """Cosine similarity of two TF-IDF vectors (already L2-normed)."""
    return sum(va.get(w, 0)*vb.get(w, 0) for w in va)

def _bigrams(tokens: List[str]) -> Set[Tuple[str,str]]:
    return {(tokens[i], tokens[i+1]) for i in range(len(tokens)-1)}

def _bigram_jaccard(ta: List[str], tb: List[str]) -> float:
    ba, bb = _bigrams(ta), _bigrams(tb)
    if not ba or not bb: return 0.0
    return len(ba & bb) / len(ba | bb)

def similarity_score(va: Dict, ta: List[str], vb: Dict, tb: List[str]) -> float:
    """
    Combined score:
      60% TF-IDF cosine   (vocabulary + term importance)
      40% bigram Jaccard  (phrase / word-order overlap)
    """
    return 0.60 * _cosine(va, vb) + 0.40 * _bigram_jaccard(ta, tb)


def group_questions(questions: List[Question],
                    threshold: float = 0.28) -> List[List[Question]]:
    """
    Union-Find grouping using NLP similarity.
    Every question is included – even those that appear only once.
    threshold=0.28 is tuned to handle paraphrasing while avoiding
    false positives across different topics.
    """
    if not questions:
        return []

    # pre-compute vectors & token lists once
    vectors  = _tfidf_vectors(questions)
    tok_list = [_tokens(q.text) for q in questions]
    n = len(questions)

    # Union-Find
    parent = list(range(n))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(x, y):
        parent[find(x)] = find(y)

    for i in range(n):
        for j in range(i+1, n):
            sc = similarity_score(vectors[i], tok_list[i],
                                  vectors[j], tok_list[j])
            if sc >= threshold:
                union(i, j)

    groups: Dict[int, List[Question]] = defaultdict(list)
    for i, q in enumerate(questions):
        groups[find(i)].append(q)

    result = list(groups.values())
    result.sort(key=lambda g: len(g), reverse=True)   # most repeated first
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  PDF GENERATION  – one output file per canonical course
# ══════════════════════════════════════════════════════════════════════════════

# colours
C_DEEP_PURPLE = colors.HexColor('#4a148c')
C_PURPLE      = colors.HexColor('#7b1fa2')
C_INDIGO      = colors.HexColor('#1a237e')
C_INDIGO_PALE = colors.HexColor('#e8eaf6')
C_RED5        = colors.HexColor('#880e4f')
C_RED4        = colors.HexColor('#b71c1c')
C_RED3        = colors.HexColor('#e53935')
C_ORANGE2     = colors.HexColor('#e65100')
C_GREY1       = colors.HexColor('#757575')
C_GREY_PALE   = colors.HexColor('#f5f5f5')


def _xml(text: str) -> str:
    t = str(text)
    t = t.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', t)

def _clean(text: str) -> str:
    text = re.sub(r'\bP\.T\.O\.?\b', '', text, flags=re.I)
    text = re.sub(r'\s{2,}', ' ', text).strip()
    return text[:900] + "…" if len(text) > 900 else text

def _freq_color(freq: int) -> colors.Color:
    if freq >= 5: return C_RED5
    if freq >= 4: return C_RED4
    if freq >= 3: return C_RED3
    if freq >= 2: return C_ORANGE2
    return C_GREY1


def build_pdf(course_code: str,
              course_name: str,
              question_groups: List[List[Question]],
              all_papers: List[SubPaper],
              output_path: str,
              show_frequency: bool = True):

    # ── per-page footer via canvas callback ────────────────────────────────
    FOOTER_TEXT = "Question Sorter  |  Made by: Ankit Sigdel"

    def _draw_page_footer(canv, doc):
        canv.saveState()
        page_w, page_h = A4
        footer_y = 1.1 * cm
        # separator line
        canv.setStrokeColor(colors.HexColor("#9fa8da"))
        canv.setLineWidth(0.5)
        canv.line(2.2*cm, footer_y + 0.35*cm, page_w - 2.2*cm, footer_y + 0.35*cm)
        # left: branding
        canv.setFont("Helvetica", 7)
        canv.setFillColor(colors.HexColor("#757575"))
        canv.drawString(2.2*cm, footer_y, FOOTER_TEXT)
        # right: page number
        canv.drawRightString(page_w - 2.2*cm, footer_y,
                             f"Page {doc.page}")
        # centre: course
        canv.drawCentredString(page_w / 2, footer_y,
                               f"{course_code}  –  {course_name}")
        canv.restoreState()

    class _KUDoc(BaseDocTemplate):
        def __init__(self, *a, **kw):
            BaseDocTemplate.__init__(self, *a, **kw)
            frame = Frame(
                self.leftMargin, self.bottomMargin,
                self.width, self.height, id="normal"
            )
            self.addPageTemplates([
                PageTemplate(id="All", frames=frame,
                             onPage=_draw_page_footer)
            ])

    doc = _KUDoc(
        output_path, pagesize=A4,
        leftMargin=2.2*cm, rightMargin=2.2*cm,
        topMargin=2.5*cm,  bottomMargin=2.5*cm,
        title=f"KU Question Bank – {course_code}",
        author="KU Question Sorter v2  |  Made by: Ankit Sigdel",
    )

    ST = getSampleStyleSheet()
    def ps(name, **kw):
        return ParagraphStyle(name, parent=ST['Normal'], **kw)

    S_TITLE    = ps('T',  fontSize=20, fontName='Helvetica-Bold',
                    textColor=C_INDIGO, alignment=TA_CENTER, spaceAfter=4)
    S_SUB      = ps('S',  fontSize=11, textColor=C_GREY1,
                    alignment=TA_CENTER, spaceAfter=4)
    S_SEC      = ps('SH', fontSize=12, fontName='Helvetica-Bold',
                    textColor=colors.white, backColor=C_INDIGO,
                    spaceBefore=14, spaceAfter=4,
                    leftIndent=-0.3*cm, rightIndent=-0.3*cm, borderPad=5)
    S_QNUM     = ps('QN', fontSize=10, fontName='Helvetica-Bold',
                    textColor=C_INDIGO, spaceBefore=5, spaceAfter=1)
    S_QTEXT    = ps('QT', fontSize=10, leading=14, leftIndent=0.6*cm, spaceAfter=3)
    S_MARKS    = ps('QM', fontSize=8,  textColor=C_GREY1,
                    leftIndent=0.6*cm, spaceAfter=2)
    S_SRC      = ps('SR', fontSize=8,  textColor=C_GREY1, fontName='Helvetica-Oblique',
                    leftIndent=0.6*cm, spaceAfter=5)
    S_FOOT     = ps('FT', fontSize=7.5, textColor=C_GREY1, alignment=TA_CENTER)
    S_LEGEND   = ps('LG', fontSize=8.5, textColor=colors.HexColor('#37474f'),
                    backColor=colors.HexColor('#fff9c4'), borderPad=5, spaceAfter=8)

    story = []

    # ─── Cover page ──────────────────────────────────────────────────────────
    story += [
        Spacer(1, 0.8*cm),
        Paragraph("KATHMANDU UNIVERSITY", S_TITLE),
        Paragraph("Sorted Question Bank", S_SUB),
        Spacer(1, 0.3*cm),
        HRFlowable(width="100%", thickness=2, color=C_INDIGO, spaceAfter=10),
    ]

    story.append(Paragraph(
        f"<b>Course:</b>  {_xml(course_code)}  –  {_xml(course_name)}", ST['Normal']))

    course_papers = [p for p in all_papers if p.course_code == course_code]
    total_q   = sum(len(g) for g in question_groups)
    dates     = sorted(set(p.exam_date for p in course_papers if p.exam_date
                            and len(p.exam_date) < 60))          # skip garbage dates

    # Format dates: one per line so they never overflow the table cell
    if dates:
        dates_cell = Paragraph("<br/>".join(_xml(d) for d in dates),
                               ParagraphStyle("DC", parent=ST["Normal"],
                                              fontSize=8, leading=11))
    else:
        dates_cell = "—"

    if show_frequency:
        unique_q = len(question_groups)
        repeated = sum(1 for g in question_groups if len(g) > 1)
        max_freq = max((len(g) for g in question_groups), default=1)
        stats = [
            ["Statistic",                      "Value"],
            ["Total questions (all sittings)",  str(total_q)],
            ["Unique question topics",          str(unique_q)],
            ["Topics repeated 2+ times",        str(repeated)],
            ["Highest repeat count",            str(max_freq)],
            ["Exam sittings analysed",          str(len(course_papers))],
            ["Exam dates",                      dates_cell],
        ]
    else:
        stats = [
            ["Statistic",                      "Value"],
            ["Total questions (all sittings)",  str(total_q)],
            ["Exam sittings analysed",          str(len(course_papers))],
            ["Exam dates",                      dates_cell],
        ]
    st = Table(stats, colWidths=[9.5*cm, 6.5*cm])
    st.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0), C_INDIGO),
        ('TEXTCOLOR',     (0,0),(-1,0), colors.white),
        ('FONTNAME',      (0,0),(-1,0), 'Helvetica-Bold'),
        ('FONTSIZE',      (0,0),(-1,-1), 8.5),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [C_INDIGO_PALE, colors.white]),
        ('GRID',          (0,0),(-1,-1), 0.4, colors.HexColor('#9fa8da')),
        ('LEFTPADDING',   (0,0),(-1,-1), 5),
        ('RIGHTPADDING',  (0,0),(-1,-1), 5),
        ('TOPPADDING',    (0,0),(-1,-1), 3),
        ('BOTTOMPADDING', (0,0),(-1,-1), 3),
    ]))
    story += [Spacer(1,0.4*cm), st, Spacer(1,0.4*cm)]

    story.append(HRFlowable(width="100%", thickness=0.5, color=C_GREY1))
    story.append(Spacer(1,0.3*cm))
    if show_frequency:
        story.append(Paragraph(
            "Colour of the frequency badge: "
            "<font color='#880e4f'><b> ■ 5+ </b></font>  "
            "<font color='#b71c1c'><b> ■ 4 </b></font>  "
            "<font color='#e53935'><b> ■ 3 </b></font>  "
            "<font color='#e65100'><b> ■ 2 </b></font>  "
            "<font color='#757575'><b> ■ 1 </b></font>  "
            "  |  Sorted: most repeated → least repeated.  "
            "Every question is included.",
            S_LEGEND
        ))
    else:
        story.append(Paragraph(
            "Questions sorted by section (A → B → C …) then by exam date.  "
            "Each question is listed once per exam paper it appeared in.",
            S_LEGEND
        ))
    story.append(PageBreak())

    # ─── Question pages ───────────────────────────────────────────────────────
    # Group by dominant section label
    by_sec: Dict[str, List[List[Question]]] = defaultdict(list)
    for grp in question_groups:
        secs = [q.section for q in grp]
        by_sec[max(set(secs), key=secs.count)].append(grp)

    for sec in sorted(by_sec.keys()):
        story.append(Paragraph(f"  SECTION {sec}", S_SEC))
        story.append(Spacer(1, 0.15*cm))

        for idx, grp in enumerate(by_sec[sec], 1):
            freq   = len(grp)
            rep    = grp[0]
            fc     = _freq_color(freq)
            text   = _clean(rep.text)

            if show_frequency:
                # header row: Q number left, frequency badge right
                hrow = Table(
                    [[Paragraph(f"<b>Q{idx}.</b>", S_QNUM),
                      Paragraph(
                          f'<font color="white"><b>  ×{freq}  </b></font>',
                          ps('B', fontSize=9, fontName='Helvetica-Bold', alignment=TA_RIGHT)
                      )]],
                    colWidths=[12.5*cm, 3.5*cm]
                )
                hrow.setStyle(TableStyle([
                    ('BACKGROUND',    (1,0),(1,0), fc),
                    ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
                    ('LEFTPADDING',   (0,0),(-1,-1), 2),
                    ('RIGHTPADDING',  (0,0),(-1,-1), 4),
                    ('TOPPADDING',    (0,0),(-1,-1), 1),
                    ('BOTTOMPADDING', (0,0),(-1,-1), 1),
                ]))
            else:
                # plain question number, no badge
                hrow = Paragraph(f"<b>Q{idx}.</b>", S_QNUM)

            # source line
            if show_frequency:
                # "Appeared in" line – deduplicate and stack neatly
                seen_dates = []
                for q in grp:
                    d = (q.exam_date or q.source_file)
                    if d and len(d) < 60 and d not in seen_dates:
                        seen_dates.append(d)
                if seen_dates:
                    if len(seen_dates) <= 4:
                        src_inner = "  ·  ".join(_xml(d) for d in seen_dates)
                    else:
                        mid = (len(seen_dates) + 1) // 2
                        left_col  = "<br/>".join(_xml(d) for d in seen_dates[:mid])
                        right_col = "<br/>".join(_xml(d) for d in seen_dates[mid:])
                        src_inner = f"{left_col}&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;{right_col}"
                    src_txt = f"Appeared in ({len(seen_dates)}x):  {src_inner}"
                else:
                    src_txt = "Source: " + _xml(grp[0].source_file)
            else:
                d = rep.exam_date or rep.source_file
                src_txt = f"Source: {_xml(d)}" if d else ""

            block = [hrow, Paragraph(_xml(text), S_QTEXT)]
            if rep.marks > 0:
                block.append(Paragraph(f"[{rep.marks} marks]", S_MARKS))
            if src_txt:
                block.append(Paragraph(f"<i>{src_txt}</i>", S_SRC))
            if show_frequency and freq > 1:
                bw = min(2 + freq*1.5, 16) * cm
                block.append(HRFlowable(width=bw, thickness=2.5,
                                        color=fc, lineCap='round', spaceAfter=2))
            block.append(Spacer(1, 0.1*cm))
            story.append(KeepTogether(block))

    # end-of-document spacer only (footer drawn per-page via canvas callback)
    story.append(Spacer(1, 1.5*cm))
    doc.build(story)
    print(f"    Saved → {output_path}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN  –  collect, canonicalise, merge, generate
# ══════════════════════════════════════════════════════════════════════════════

def _collect_pdfs(directory: str) -> List[str]:
    out = []
    for root, _, files in os.walk(directory):
        for f in sorted(files):
            if f.lower().endswith('.pdf'):
                out.append(os.path.join(root, f))
    return out


def run(input_dir: str, output_dir: str,
        direct_course: Optional[str] = None,
        process_all: bool = False):

    print("\n" + "="*64)
    print("  KU QUESTION PAPER SORTER  v2")
    print("="*64)

    pdfs = _collect_pdfs(input_dir)
    if not pdfs:
        print(f"\n✗  No PDF files found in: {input_dir}"); return

    print(f"\n  Found {len(pdfs)} PDF file(s)\n")
    print("  Extracting text …\n")

    # ── step 1: process all PDFs ────────────────────────────────────────────
    all_papers: List[SubPaper] = []
    for pdf in pdfs:
        try:
            all_papers.extend(process_pdf(pdf))
        except Exception as e:
            print(f"  ✗ {os.path.basename(pdf)}: {e}")

    if not all_papers:
        print("\n✗  No papers processed."); return

    # ── step 2: build canonical course index ────────────────────────────────
    #  Key = canonical code; value = list of all SubPapers for that course
    course_index: Dict[str, List[SubPaper]] = defaultdict(list)
    for p in all_papers:
        if p.course_code not in ("UNKNOWN", ""):
            course_index[p.course_code].append(p)

    # ── step 3: course selection ─────────────────────────────────────────────
    if process_all:
        selected = list(course_index.keys())

    elif direct_course:
        canon = canonicalise(direct_course)
        # exact match first
        if canon in course_index:
            selected = [canon]
        else:
            # prefix fuzzy
            matches = [c for c in course_index
                       if c.startswith(canon[:4]) or canon.startswith(c[:4])]
            if not matches:
                print(f"\n✗  Course '{direct_course}' not found.")
                print("   Available:", ", ".join(sorted(course_index)[:20]))
                return
            selected = matches
            if len(selected) > 1:
                print(f"  Note: '{direct_course}' matched multiple codes: "
                      f"{', '.join(selected)}")
                print("  All will be merged into the most likely canonical.\n")
    else:
        # interactive menu
        print("\n" + "="*64)
        print("  COURSES DETECTED")
        print("="*64)
        sorted_courses = sorted(course_index.items(),
                                key=lambda x: (-len(x[1]),
                                               -sum(len(p.questions) for p in x[1])))
        for i, (code, papers) in enumerate(sorted_courses, 1):
            name    = CATALOGUE.get(code, papers[0].course_name or "")
            total_q = sum(len(p.questions) for p in papers)
            dates   = sorted({p.exam_date for p in papers
                              if p.exam_date and len(p.exam_date) < 60})[:3]
            dstr    = ", ".join(dates) + ("…" if
                      sum(1 for p in papers if p.exam_date) > 3 else "")
            print(f"  {i:3}.  {code:<12}  {name:<35}  "
                  f"{len(papers):3} sittings  {total_q:4} Qs  [{dstr}]")

        print(f"\n    0.  Process ALL courses")
        print(f"    q.  Quit\n")

        while True:
            try:
                raw = input("  Enter course code or number: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nBye!"); return
            if raw.lower() == 'q':
                print("Bye!"); return
            if raw == '0':
                selected = [c for c, _ in sorted_courses]; break
            if raw.isdigit():
                idx = int(raw) - 1
                if 0 <= idx < len(sorted_courses):
                    selected = [sorted_courses[idx][0]]; break
                print("  Invalid number."); continue
            # try by code
            canon = canonicalise(raw)
            if canon in course_index:
                selected = [canon]; break
            # prefix fuzzy
            fuzz = [c for c in course_index
                    if c.startswith(canon[:4]) or canon.startswith(c[:4])]
            if fuzz:
                selected = fuzz
                if len(fuzz) > 1:
                    print(f"  Matched multiple codes: {', '.join(fuzz)}")
                    print("  They will each produce ONE separate output PDF.")
                break
            print(f"  '{raw}' not found. Try again.")

    # ── step 4: generate one PDF per selected canonical code ─────────────────
    os.makedirs(output_dir, exist_ok=True)

    for code in selected:
        papers   = course_index.get(code, [])
        if not papers: continue
        all_qs   = [q for p in papers for q in p.questions]
        if not all_qs:
            print(f"\n  ⚠  No questions for {code}"); continue

        name = CATALOGUE.get(code, papers[0].course_name or code)
        print(f"\n{'='*64}")
        print(f"  GENERATING:  {code}  –  {name}")
        print(f"{'='*64}")
        print(f"  Questions collected  : {len(all_qs)}")

        # Sort all questions by section, then exam date (or source file), then
        # question number. Each question is its own group so duplicates are
        # preserved as-is.
        def _sort_key(q: Question):
            m = re.match(r'(\d+)', q.number)
            num = int(m.group(1)) if m else 0
            return (q.section, q.exam_date or getattr(q, "source_file", "") or "", num)

        sorted_qs = sorted(all_qs, key=_sort_key)
        groups = [[q] for q in sorted_qs]
        print(f"  Questions (by section): {len(groups)}")

        safe = re.sub(r'[^\w\-]', '_', code)
        outf = os.path.join(output_dir, f"QuestionBank_{safe}.pdf")
        print(f"  Building PDF …")
        build_pdf(code, name, groups, all_papers, outf, show_frequency=False)

    print(f"\n{'='*64}")
    print(f"  Done!  Output folder: {output_dir}")
    print(f"{'='*64}\n")


# ── entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="KU Question Paper Sorter v2 – one PDF per course, sorted by frequency"
    )
    ap.add_argument("input_dir", nargs="?", default=".",
                    help="Folder containing exam PDFs (default: current dir)")
    ap.add_argument("--output","-o", default="./output",
                    help="Output folder (default: ./output)")
    ap.add_argument("--course","-c", default=None,
                    help="Process a specific course code only")
    ap.add_argument("--all","-a", action="store_true",
                    help="Process every detected course without prompting")
    a = ap.parse_args()

    d = os.path.abspath(a.input_dir)
    if not os.path.isdir(d):
        sys.exit(f"✗  Not a directory: {d}")
    run(d, os.path.abspath(a.output), a.course, a.all)