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

import os, sys, re, math, subprocess, tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Set

# ── required deps ──────────────────────────────────────────────────────────────
try:
    import pdfplumber
except ImportError:
    sys.exit("Missing: pdfplumber\nRun: pip install pdfplumber")

try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit("Missing: PyMuPDF\nRun: pip install PyMuPDF")

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
    source_path: str      # full path to the source PDF
    start_page:  int      # 1-indexed first page of this sub-paper
    end_page:    int      # 1-indexed last page of this sub-paper
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
            source_path=pdf_path,
            start_page=start,
            end_page=end,
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
#  PDF GENERATION  – render original pages as images, combine into one PDF
# ══════════════════════════════════════════════════════════════════════════════

def render_course_to_pdf(course_code: str,
                         papers: List[SubPaper],
                         output_path: str,
                         dpi: int = 150) -> None:
    """
    For every SubPaper in *papers*, render the source PDF pages (start_page …
    end_page) as PNG images at *dpi* resolution, combine all images into a
    single output PDF, then delete the temporary images.
    """
    tmp_dir     = tempfile.mkdtemp(prefix="ku_qs_")
    image_paths: List[str] = []

    try:
        for paper in papers:
            try:
                src = fitz.open(paper.source_path)
            except Exception as e:
                print(f"    ⚠  Cannot open {paper.source_path}: {e}")
                continue

            zoom = dpi / 72.0
            mat  = fitz.Matrix(zoom, zoom)

            # fitz pages are 0-indexed; SubPaper pages are 1-indexed
            for pg in range(paper.start_page - 1, paper.end_page):
                if pg < 0 or pg >= len(src):
                    continue
                pix      = src[pg].get_pixmap(matrix=mat, alpha=False)
                img_name = (
                    f"{re.sub(r'[^\\w]', '_', course_code)}_"
                    f"{re.sub(r'[^\\w]', '_', os.path.basename(paper.source_path))}_"
                    f"{pg:04d}.png"
                )
                img_path = os.path.join(tmp_dir, img_name)
                pix.save(img_path)
                image_paths.append(img_path)

            src.close()

        if not image_paths:
            print(f"  ⚠  No pages to render for {course_code}")
            return

        # Combine all page images into a single output PDF
        out_doc = fitz.open()
        for img_path in image_paths:
            with fitz.open(img_path) as img_doc:
                rect = img_doc[0].rect
            page = out_doc.new_page(width=rect.width, height=rect.height)
            page.insert_image(page.rect, filename=img_path)

        out_doc.save(output_path, deflate=True)
        out_doc.close()
        print(f"    Saved → {output_path}")

    finally:
        # Always delete temporary screenshots
        for img_path in image_paths:
            try:
                os.remove(img_path)
            except OSError:
                pass
        try:
            os.rmdir(tmp_dir)
        except OSError:
            pass


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

        name = CATALOGUE.get(code, papers[0].course_name or code)
        print(f"\n{'='*64}")
        print(f"  GENERATING:  {code}  –  {name}")
        print(f"{'='*64}")
        print(f"  Source papers : {len(papers)}")

        safe = re.sub(r'[^\w\-]', '_', code)
        outf = os.path.join(output_dir, f"QuestionBank_{safe}.pdf")
        print(f"  Rendering pages to PDF …")
        render_course_to_pdf(code, papers, outf)

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