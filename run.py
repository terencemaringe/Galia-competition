import os, re, json, csv, datetime, requests, yaml, html, unicodedata
from bs4 import BeautifulSoup
import feedparser

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"].strip()

# =========================
# PROMPTS (INTEGRES)
# =========================

SYSTEM_PROMPT = """Tu es un analyste investissement infrastructure biogaz (Pologne) spécialisé dans la veille concurrentielle (fonds, debt, equity) et dans la structuration de données de deals.

Ta mission : à partir d’un texte brut (article, communiqué, post), détecter s’il décrit un deal ou une news “fonds/GP”, puis extraire des champs structurés.

CONTRAINTES DE SORTIE
- Réponds UNIQUEMENT avec un JSON valide, sans texte additionnel, sans markdown.
- Si l’information n’existe pas, mets null (ne pas inventer).
- Date au format ISO 8601 : YYYY-MM-DD si possible, sinon null.
- Montants : si EUR explicitement mentionné => amount_eur numérique (sans séparateurs). Si devise non EUR et conversion incertaine => currency=<devise> et amount_eur=null.
- Si plusieurs items sont décrits, retourne une liste dans "deals".
- Si ce n’est ni un deal ni une news fonds/GP pertinente :
  {"is_deal": false, "rejection_reason": "...", "deals": []}

DÉFINITIONS DES ACTEURS (CRITIQUE)
- investor = le financeur/investisseur/lender (fonds, banque, gestionnaire d’actifs) qui réalise l’opération.
- project_or_company = la société / plateforme / SPV / projet financé(e) (le bénéficiaire).
- Ne confonds jamais le bénéficiaire avec le financeur.
- Si le financeur n’est pas mentionné explicitement (ex: “Company X raised €Y” sans citer d’investisseurs/lenders) :
  - investor = null
  - project_or_company = Company X
- investor ne doit PAS être un développeur/IPP/constructeur (sauf si c’est clairement lui qui investit via un véhicule d’investissement).
- Ne pas “deviner” un investor.

SEGMENTS À PRODUIRE (segment) + RÈGLES

1) DETTE BRIDGE_GAS_VERT
Déclenchement :
- maturité <= 5 ans OU mention bridge / short-term / construction facility / term loan court terme (<=60 mois).
- dette lié à biométhane / RNG / biogaz / méthanisation / renewable gas / e-methane
Sous-segment (bucket) :
- "100_300" si 100 <= montant < 300
- "50_100" si 50 <= montant < 100
- "20_50" si 20 <= montant < 50
- "OTHER" sinon ou montant inconnu
Champs clés : maturity_years, pricing (margin/spread si mentionné), stage.

2) DETTE SENIOR_GAZ_VERT
Déclenchement :
- maturité > 5 ans OU mention senior / long-term / bank facility / bank loan long term (>60 mois).
- dette lié à biométhane / RNG / biogaz / méthanisation / renewable gas / e-methane
Sous-segment (bucket) :
- "100_300" si 100 <= montant < 300
- "50_100" si 50 <= montant < 100
- "20_50" si 20 <= montant < 50
- "OTHER" sinon ou montant inconnu
Champs clés : maturity_years, pricing (margin/spread si mentionné), stage.

3) EQUITY_GAZ_VERT
Déclenchement :
- equity lié à biométhane / RNG / biogaz / méthanisation / renewable gas / e-methane
- montant <= 100 M€ si mentionné (sinon accepter)
Règle out_of_scope :
- si montant > 100 M€ => out_of_scope=true

4) FUNDRAISING_NEWS (STRICT)
IMPORTANT : cette catégorie est UNIQUEMENT pour les news de fonds/GP (gestionnaires), pas pour les sociétés/projets.
Déclenchement :
- lancement de fonds ou plateforme, first close, final close, fundraising update d’un fonds, target size, oversubscribed, AUM update
- stratégie de fonds / création d’une nouvelle stratégie infra liée à biométhane / RNG / biogaz / méthanisation / renewable gas / e-methane
- gros mouvement d’équipe chez un GP (head of infra, partner, CIO infra…)
- M&A / partenariat concernant un gestionnaire d’actifs (asset manager / GP)
Sous-segment :
- "FUND_LAUNCH", "FIRST_CLOSE", "FINAL_CLOSE", "FUNDRAISING_UPDATE", "AUM_UPDATE", "TEAM_MOVE", "M_AND_A_MANAGER", "STRATEGY_UPDATE"

5) DEVELOPER_NEWS
Déclenchement :
- lancement d'une plateforme de développement liée au biométhane / RNG / biogaz / méthanisation / renewable gas / e-methane
- stratégie de développement / création d’une nouvelle stratégie liée au biométhane / RNG / biogaz / méthanisation / renewable gas / e-methane
- gros mouvement d’équipe chez un développeur (head of project finance, CEO, CIO, partner...)

EXCLUSION FUNDRAISING_NEWS (OBLIGATOIRE)
- Si l’objet principal est une société/projet qui lève de la dette ou de l’equity (corporate funding round, IPO, project financing), ce n’est PAS FUNDRAISING_NEWS.
  => classer dans DEVELOPER_NEWS ou EQUITY_GAZ_VERT ou DETTE BRIDGE_GAS_VERT ou DETTE SENIOR_GAZ_VERT selon le cas.

ADVISERS / CONSEILS (à renseigner si mentionné)
- advisor_financial : nom(s) du conseil financier / bank / M&A advisor / debt advisor / arranger
- advisor_legal : nom(s) du cabinet juridique
- advisor_technical : nom(s) du conseil technique / engineer / due diligence provider
Si non mentionné => null.

FILTRE GÉOGRAPHIQUE
- Ne conserver que les deals situés en Pologne..
- Si le deal est hors Pologne, retourne is_deal=false.

CHAMPS À EXTRAIRE PAR ITEM (deal/news)
- deal_date
- investor
- segment
- sub_segment
- project_or_company
- country
- technology (CHP/Biomethane injection/Heat/Landfill gas)
- amount_eur
- currency
- maturity_years
- stage
- pricing
- advisor_financial
- advisor_legal
- advisor_technical
- fund_name
- fund_size_target_eur
- fund_size_raised_eur
- fund_close_type (first/final si pertinent)
- aum_eur
- source_title
- source_url
- confidence (0 à 1)
- out_of_scope (true/false/null)

FORMAT DE SORTIE
{
  "is_deal": true/false,
  "rejection_reason": null ou string,
  "deals": [ { ...item... }, ... ]
}
"""

NEWSLETTER_PROMPT = """Tu es un rédacteur/analyste pour une newsletter hebdomadaire de veille concurrentielle infra gaz vert en Pologne.

Tu reçois un JSON contenant:
- week
- items : liste d’items structurés (deals + news fonds/GP)

Objectif : produire un email HTML en français, lisible par une équipe d’investissement.

CONTRAINTES
- Réponds UNIQUEMENT avec du HTML (pas de markdown).
- Ne pas inventer. Si information manquante: "n/a".
- Utiliser des tableaux HTML.
- La colonne "Conseil (Fin/Legal/Tech)" doit afficher un texte concaténé :
  - "Fin: <advisor_financial> | Legal: <advisor_legal> | Tech: <advisor_technical>"
  - Si rien: "n/a".

STRUCTURE DE L’EMAIL (5 sections)

1) DETTE BRIDGE_GAS_VERT
- sous-sections : 100–300 M€, 50–100 M€, 20–50 M€
- Colonnes tableau:
  Date | Concurrent | Projet/Société | Pays | Techno | Montant (EUR) | Pricing | Conseil (Fin/Legal/Tech) | Maturité | Stade | Source

2) DETTE SENIOR_GAZ_VERT
- sous-sections : 100–300 M€, 50–100 M€, 20–50 M€
- Colonnes tableau:
  Date | Concurrent | Projet/Société | Pays | Techno | Montant (EUR) | Pricing | Conseil (Fin/Legal/Tech) | Maturité | Stade | Source

3) EQUITY_GAZ_VERT
- sous-sections : PV, Wind, BESS
- Colonnes tableau (mêmes que ci-dessus, maturité peut être "n/a")

4) FUNDRAISING_NEWS (STRICT)
- Inclure uniquement les items segment="FUNDRAISING_NEWS"
- Colonnes tableau:
  Date | Acteur (GP/fonds) | Type news | FundName | Montant (raised/target/AUM) | Conseil (Fin/Legal/Tech) | Source
- Montant = fund_size_raised_eur sinon fund_size_target_eur sinon aum_eur sinon "n/a"

5) DEVELOPER_NEWS
- Inclure uniquement les items segment="FUNDRAISING_NEWS"
- Colonnes tableau:
  Date | Acteur (GP/fonds) | Type news | FundName | Montant (raised/target/AUM) | Conseil (Fin/Legal/Tech) | Source
- Montant = fund_size_raised_eur sinon fund_size_target_eur sinon aum_eur sinon "n/a"

INSIGHTS (fin de mail)
Ajoute 5 bullet points:
- 2 sur les dernières évolutions règlementaires
- 1 sur dette bridge ou senior (taille/pricing/géographie)
- 1 sur equity gas vert (tech, zones, appétit)
- 1 sur fundraising fonds/GP (activité, tailles, signaux marché)

Sans inventer: si pas assez de data, écrire des insights prudents basés uniquement sur les items présents.
"""

# =========================
# FILTERS (LOW COST)
# =========================
KEYWORDS = [
    # finance / transactions
    "financing","financed","facility","loan","debt","bridge","term loan","maturity","refinancing",
    "arranger","bookrunner","mandated lead arranger","mla","bps","basis points","euribor","sofr",
    "acquires","acquired","investment","invests","equity","stake","raises","raised","funding","series","ipo",
    "portfolio","platform","project finance","green loan","sustainability-linked",
    # fundraising funds/GP
    "fund","first close","final close","closing","aum","asset manager","limited partner","lp","gp",
    # tech keywords
    "hybrid","biomethane","rng","renewable gas","biogas","anaerobic", "CH4", "gas injection", "CHP", "combined heat & power", "landfill gas"
]

def looks_relevant(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in KEYWORDS)


ALLOWED_GEOS = {
"poland", "pologne", "poslka"
}

EMPTY_GEO_VALUES = {"", "n/a", "na", "null", "none", "unknown", "inconnu", "non precise"}

# =========================
# Helpers
# =========================

def iso_week(d=None):
    d = d or datetime.date.today()
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def normalize_geo(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = text.replace("-", " ")
    text = re.sub(r"[()]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def geography_in_scope(country: str | None) -> bool:
    normalized = normalize_geo(country)
    if normalized in EMPTY_GEO_VALUES:
        return False
    if normalized in ALLOWED_GEOS:
        return True

    parts = [
        part.strip()
        for part in re.split(r"[,/;|]| and | et ", normalized)
        if part.strip()
    ]
    if not parts:
        return False
    return all(part in ALLOWED_GEOS for part in parts)

def openai_chat(messages, model="gpt-4.1-mini", timeout=180, max_retries=3):
    payload = {"model": model, "temperature": 0, "messages": messages}
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json=payload,
                timeout=timeout,
            )
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            last_err = e
            import time
            time.sleep(5 * attempt)
    raise last_err

def fetch_html(url):
    try:
        r = requests.get(
            url,
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
        if r.status_code in (401, 403, 406, 429):
            return ""
        r.raise_for_status()
        return r.text
    except Exception:
        return ""

def extract_text_from_article(url, max_chars=8000):
    try:
        html = fetch_html(url)
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = " ".join(soup.get_text(" ").split())
        return text[:max_chars]
    except Exception:
        return ""

def parse_rss(feed_url, limit=60):
    d = feedparser.parse(feed_url)
    out = []
    for e in d.entries[:limit]:
        title = (e.get("title", "") or "")[:160]
        link = e.get("link", "") or ""
        summary = e.get("summary", "") or ""
        if not summary and "content" in e and e.content:
            try:
                summary = e.content[0].value or ""
            except Exception:
                summary = ""
        if link:
            out.append((title, link, summary))
    return out

def extract_items(title, url, text):
    user = {"source_title": title, "source_url": url, "content": text}
    content = openai_chat(
        [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        timeout=180,
        max_retries=3,
    )
    try:
        return json.loads(content)
    except Exception:
        m = re.search(r"\{.*\}", content, flags=re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
        return {"is_deal": False, "rejection_reason": "invalid_json", "deals": []}

def concat_conseil(advisor_fin, advisor_legal, advisor_tech):
    parts = []
    if advisor_fin:
        parts.append(f"Fin: {advisor_fin}")
    if advisor_legal:
        parts.append(f"Legal: {advisor_legal}")
    if advisor_tech:
        parts.append(f"Tech: {advisor_tech}")
    return " | ".join(parts) if parts else None

def format_number(value):
    if value in (None, "", "n/a"):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        formatted = f"{value:,.2f}"
        return formatted.rstrip("0").rstrip(".")
    if isinstance(value, str):
        raw = value.strip().replace(",", "").replace(" ", "")
        if not raw:
            return value
        try:
            if "." in raw:
                numeric = float(raw)
                if numeric.is_integer():
                    return f"{int(numeric):,}"
                formatted = f"{numeric:,.2f}"
                return formatted.rstrip("0").rstrip(".")
            return f"{int(raw):,}"
        except ValueError:
            return value
    return value

def format_row_numbers(row):
    for key in ("Amount_EUR", "FundSizeTarget_EUR", "FundSizeRaised_EUR", "AUM_EUR"):
        row[key] = format_number(row.get(key))
    return row

def make_fallback_newsletter_html(week, rows):
    def esc(x):
        return (str(x) if x is not None else "n/a").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    parts = [f"<h1>Newsletter Galia Competition — Week {week}</h1>"]
    parts.append("<p><b>Note:</b> génération IA indisponible. Version fallback.</p>")

    def subsection(title):
        parts.append(f"<div class='subsection' style='margin-top:12px;font-weight:bold;color:#1a4d8f;'>{esc(title)}</div>")

    def table_deals(title, filt):
        subset = [r for r in rows if filt(r)]
        subsection(f"{title} ({len(subset)})")
        parts.append("<table border='1' cellspacing='0' cellpadding='6' style='border-collapse:collapse;width:100%;margin-bottom:18px;'>")
        parts.append(
            "<tr>"
            "<th>Date</th><th>Concurrent</th><th>Projet/Société</th><th>Pays</th><th>Techno</th>"
            "<th>Montant (EUR)</th><th>Pricing</th><th>Conseil (Fin/Legal/Tech)</th><th>Maturité</th><th>Stade</th><th>Source</th>"
            "</tr>"
        )
        if not subset:
            parts.append("<tr><td colspan='11' style='text-align:center;'>Aucune donnée disponible</td></tr>")
            parts.append("</table>")
            return
        for r in subset[:80]:
            parts.append(
                "<tr>"
                f"<td>{esc(r.get('DealDate'))}</td>"
                f"<td>{esc(r.get('Competitor'))}</td>"
                f"<td>{esc(r.get('ProjectOrCompany'))}</td>"
                f"<td>{esc(r.get('Country'))}</td>"
                f"<td>{esc(r.get('Technology'))}</td>"
                f"<td>{esc(r.get('Amount_EUR'))}</td>"
                f"<td>{esc(r.get('Pricing'))}</td>"
                f"<td>{esc(r.get('Conseil'))}</td>"
                f"<td>{esc(r.get('Maturity_Years'))}</td>"
                f"<td>{esc(r.get('Stage'))}</td>"
                f"<td><a href='{esc(r.get('SourceURL'))}'>Source</a></td>"
                "</tr>"
            )
        parts.append("</table>")

    def table_fundraising(title, filt):
        subset = [r for r in rows if filt(r)]
        subsection(f"{title} ({len(subset)})")
        parts.append("<table border='1' cellspacing='0' cellpadding='6' style='border-collapse:collapse;width:100%;margin-bottom:18px;'>")
        parts.append("<tr><th>Date</th><th>Acteur</th><th>Type news</th><th>FundName</th><th>Montant</th><th>Conseil</th><th>Source</th></tr>")
        if not subset:
            parts.append("<tr><td colspan='7' style='text-align:center;'>Aucune donnée disponible</td></tr>")
            parts.append("</table>")
            return
        for r in subset[:80]:
            amount = r.get("FundSizeRaised_EUR") or r.get("FundSizeTarget_EUR") or r.get("AUM_EUR")
            parts.append(
                "<tr>"
                f"<td>{esc(r.get('DealDate'))}</td>"
                f"<td>{esc(r.get('Competitor'))}</td>"
                f"<td>{esc(r.get('SubSegment'))}</td>"
                f"<td>{esc(r.get('FundName'))}</td>"
                f"<td>{esc(amount)}</td>"
                f"<td>{esc(r.get('Conseil'))}</td>"
                f"<td><a href='{esc(r.get('SourceURL'))}'>Source</a></td>"
                "</tr>"
            )
        parts.append("</table>")

    parts.append("<h2>1) DETTE BRIDGE GAZ VERT</h2>")
    table_deals("100–300 M€", lambda r: r.get("Segment") == "DETTE BRIDGE_GAS_VERT" and r.get("SubSegment") == "100_300")
    table_deals("50–100 M€", lambda r: r.get("Segment") == "DETTE BRIDGE_GAS_VERT" and r.get("SubSegment") == "50_100")
    table_deals("20–50 M€", lambda r: r.get("Segment") == "DETTE BRIDGE_GAS_VERT" and r.get("SubSegment") == "20_50")

    parts.append("<h2>2) DETTE SENIOR GAZ VERT</h2>")
    table_deals("PV", lambda r: r.get("Segment") == "DETTE SENIOR_GAZ_VERT" and r.get("SubSegment") == "PV")
    table_deals("Wind", lambda r: r.get("Segment") == "DETTE SENIOR_GAZ_VERT" and r.get("SubSegment") == "WIND")
    table_deals("BESS", lambda r: r.get("Segment") == "DETTE SENIOR_GAZ_VERT" and r.get("SubSegment") == "BESS")

    parts.append("<h2>3) EQUITY GAZ VERT (&lt;100 M€)</h2>")
    table_deals("Gas vert", lambda r: r.get("Segment") == "EQUITY_GAZ_VERT")

    parts.append("<h2>4) FUNDRAISING NEWS</h2>")
    table_fundraising("Fonds/GP uniquement", lambda r: r.get("Segment") == "FUNDRAISING_NEWS")

    return "\n".join(parts)

def text_to_html(text: str) -> str:
    """Apply Eiffel brand styling to the final newsletter HTML."""
    BLEU = "#00143B"
    OCRE = "#D18F41"
    BLANC = "#FFFFFF"
    VERT = "#006660"
    BORDURE = "#D9E0EA"

    content = (text or "").strip()
    if not content:
        content = "<p>Aucune donnée disponible.</p>"

    if "<" in content and ">" in content:
        soup = BeautifulSoup(content, "html.parser")
    else:
        escaped = html.escape(content).replace("\n", "<br/>")
        soup = BeautifulSoup(f"<html><body><div>{escaped}</div></body></html>", "html.parser")

    if soup.html is None:
        html_tag = soup.new_tag("html")
        body_tag = soup.new_tag("body")
        for node in list(soup.contents):
            body_tag.append(node.extract())
        html_tag.append(body_tag)
        soup.append(html_tag)

    if soup.head is None:
        head = soup.new_tag("head")
        meta = soup.new_tag("meta", charset="UTF-8")
        head.append(meta)
        soup.html.insert(0, head)
    elif soup.head.find("meta", attrs={"charset": True}) is None:
        meta = soup.new_tag("meta", charset="UTF-8")
        soup.head.insert(0, meta)

    if soup.body is None:
        body = soup.new_tag("body")
        soup.html.append(body)
    body = soup.body

    wrapper = soup.new_tag("div")
    wrapper["style"] = (
        f"font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;"
        f"font-size:14px;line-height:1.7;color:{BLEU};max-width:1100px;"
        f"margin:0 auto;padding:34px 34px 28px 34px;background-color:{BLANC};"
        f"border:1px solid #E2E8F0;border-radius:18px;"
        f"box-shadow:0 14px 40px rgba(0,20,59,0.08);"
    )

    for tag in list(body.contents):
        wrapper.append(tag.extract())

    body.append(wrapper)
    body["style"] = (
        "margin:0;padding:28px;background-color:#EEF2F6;color:#00143B;"
    )

    if not wrapper.find("h1"):
        title = soup.new_tag("h1")
        title.string = "Newsletter Infra"
        wrapper.insert(0, title)

    for h1 in wrapper.find_all("h1"):
        h1["style"] = (
            f"color:{BLEU};font-size:28px;margin:0 0 28px 0;font-weight:800;"
            f"letter-spacing:-0.5px;padding:0 0 16px 0;border-bottom:3px solid {OCRE};"
        )

    segment_header_style = (
        f"color:{BLEU};font-size:18px;font-weight:800;margin:32px 0 16px 0;"
        f"padding:10px 14px 10px 0;border-bottom:2px solid {OCRE};"
        f"letter-spacing:0.2px;"
    )

    for h2 in wrapper.find_all("h2"):
        h2["style"] = segment_header_style

    for h3 in wrapper.find_all("h3"):
        h3["style"] = (
            f"margin:20px 0 10px 0;font-size:13px;font-weight:800;color:{VERT};"
            f"text-transform:uppercase;letter-spacing:0.6px;"
        )

    for maybe_insights in wrapper.find_all(["p", "div", "strong"]):
        if maybe_insights.get_text(" ", strip=True).lower() == "insights":
            maybe_insights.name = "h2"
            maybe_insights["style"] = segment_header_style

    table_style = (
        f"width:100%;border-collapse:separate;border-spacing:0;margin:14px 0 26px 0;"
        f"font-size:13px;background-color:{BLANC};"
        f"border:1px solid {BORDURE};border-radius:14px;overflow:hidden;"
        f"box-shadow:0 8px 22px rgba(0,20,59,0.07);"
    )
    th_style = (
        f"background-color:{BLEU};color:{BLANC};font-weight:700;text-align:left;"
        f"padding:12px 12px;border:0;border-right:1px solid #16305E;vertical-align:top;"
    )
    td_style = (
        f"padding:11px 12px;border:0;border-top:1px solid {BORDURE};"
        f"vertical-align:top;color:{BLEU};background-color:{BLANC};"
    )

    for table in wrapper.find_all("table"):
        table["style"] = table_style
        table["border"] = "0"
        table["cellspacing"] = "0"
        table["cellpadding"] = "0"

    for thead in wrapper.find_all("thead"):
        thead.attrs.pop("style", None)

    for th in wrapper.find_all("th"):
        th["style"] = th_style

    for tr in wrapper.find_all("tr"):
        tr.attrs.pop("style", None)

    for thead in wrapper.find_all("thead"):
        header_cells = thead.find_all("th")
        if header_cells:
            header_cells[0]["style"] += "border-top-left-radius:14px;"
            header_cells[-1]["style"] += "border-top-right-radius:14px;border-right:0;"

    for tbody in wrapper.find_all("tbody"):
        rows = tbody.find_all("tr", recursive=False)
        if not rows:
            colspan = 1
            table = tbody.find_parent("table")
            if table and table.find("thead"):
                colspan = max(1, len(table.find("thead").find_all("th")))
            empty_row = soup.new_tag("tr")
            empty_td = soup.new_tag("td", colspan=str(colspan))
            empty_td.string = "Aucune donnée disponible"
            empty_td["style"] = (
                f"padding:14px 12px;border-top:1px solid {BORDURE};text-align:center;"
                f"color:#5B6B84;background-color:#F8FAFC;font-style:italic;"
            )
            empty_row.append(empty_td)
            tbody.append(empty_row)
            rows = [empty_row]

        for idx, tr in enumerate(rows):
            bg = "#F8FAFC" if idx % 2 else BLANC
            cells = tr.find_all("td", recursive=False)
            for cell in cells:
                cell["style"] = td_style + f"background-color:{bg};"
            if cells:
                cells[-1]["style"] += "border-right:0;"
        if rows:
            last_cells = rows[-1].find_all("td", recursive=False)
            if last_cells:
                last_cells[0]["style"] += "border-bottom-left-radius:14px;"
                last_cells[-1]["style"] += "border-bottom-right-radius:14px;"

    for p in wrapper.find_all("p"):
        p["style"] = f"margin:10px 0;line-height:1.7;color:{BLEU};"

    for ul in wrapper.find_all("ul"):
        ul["style"] = (
            f"margin:12px 0 24px 0;padding:18px 22px 18px 36px;background-color:#F8FAFC;"
            f"border:1px solid {BORDURE};border-radius:14px;"
        )

    for li in wrapper.find_all("li"):
        li["style"] = f"margin:8px 0;color:{BLEU};padding-left:2px;"

    for a in wrapper.find_all("a"):
        href = a.get("href", "")
        a["href"] = href
        a["style"] = f"color:{OCRE};text-decoration:none;font-weight:600;"

    for strong in wrapper.find_all("strong"):
        if strong.parent and strong.parent.name != "th":
            strong["style"] = f"color:{BLEU};font-weight:700;"

    footer = soup.new_tag("div")
    footer["style"] = (
        f"margin-top:34px;padding-top:18px;border-top:1px solid {OCRE};"
        f"font-size:12px;color:{VERT};"
    )
    footer_p = soup.new_tag("p")
    footer_p["style"] = "margin:5px 0;"
    footer_p.append("Ce rapport a été généré automatiquement. ")


    return str(soup)

# =========================
# Main
# =========================

def main():
    with open("sources.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    week = iso_week()
    sources = cfg.get("sources", [])
    print(f"Loaded {len(sources)} sources")

    candidates = []
    for s in sources:
        try:
            candidates += parse_rss(s["url"], limit=60)
        except Exception as e:
            print(f"RSS parse failed for {s.get('name')} : {e}")

    print(f"Collected {len(candidates)} RSS items")

    # Dedup by URL
    seen = set()
    uniq = []
    for t, u, s in candidates:
        if u in seen:
            continue
        seen.add(u)
        uniq.append((t, u, s))

    print(f"Unique URLs: {len(uniq)}")
    print("Starting extraction loop...")

    rows = []
    skipped_geo = 0
    max_urls_to_process = 200  # << augmente le remplissage
    for title, url, summary in uniq[:max_urls_to_process]:
        text = extract_text_from_article(url)

        # fallback RSS summary if HTML blocked
        if (not text or len(text) < 200) and summary and len(summary) >= 120:
            text = summary

        if not text or len(text) < 120:
            continue

        # cheap keyword filter to avoid unnecessary LLM calls
        if not looks_relevant((title or "") + " " + (text or "")):
            continue

        try:
            extracted = extract_items(title, url, text)
        except Exception as e:
            print(f"Extract failed: {url} :: {e}")
            continue

        if not extracted or not extracted.get("is_deal"):
            continue

        for d in extracted.get("deals", []):
            if not geography_in_scope(d.get("country")):
                skipped_geo += 1
                continue

            conseil = concat_conseil(d.get("advisor_financial"), d.get("advisor_legal"), d.get("advisor_technical"))

            rows.append(
                format_row_numbers(
                    {
                        "Week": week,
                        "DealDate": d.get("deal_date"),
                        "Segment": d.get("segment"),
                        "SubSegment": d.get("sub_segment"),
                        "Competitor": d.get("competitor"),
                        "Investor": d.get("investor"),
                        "ProjectOrCompany": d.get("project_or_company"),
                        "Country": d.get("country"),
                        "Technology": d.get("technology"),
                        "Amount_EUR": d.get("amount_eur"),
                        "Currency": d.get("currency"),
                        "Pricing": d.get("pricing"),
                        "Conseil": conseil,
                        "Maturity_Years": d.get("maturity_years"),
                        "Stage": d.get("stage"),
                        "FundName": d.get("fund_name"),
                        "FundSizeTarget_EUR": d.get("fund_size_target_eur"),
                        "FundSizeRaised_EUR": d.get("fund_size_raised_eur"),
                        "FundCloseType": d.get("fund_close_type"),
                        "AUM_EUR": d.get("aum_eur"),
                        "SourceTitle": d.get("source_title") or title,
                        "SourceURL": d.get("source_url") or url,
                        "Confidence": d.get("confidence"),
                        "OutOfScope": d.get("out_of_scope", False),
                        "Notes": None,
                    }
                )
            )

    print(f"Extracted items: {len(rows)}")
    print(f"Skipped out-of-scope geographies: {skipped_geo}")

    os.makedirs("output", exist_ok=True)

    cols = [
        "Week","DealDate","Segment","SubSegment","Competitor","ProjectOrCompany","Country","Technology",
        "Amount_EUR","Currency","Pricing","Conseil","Maturity_Years","Stage",
        "FundName","FundSizeTarget_EUR","FundSizeRaised_EUR","FundCloseType","AUM_EUR",
        "SourceTitle","SourceURL","Confidence","OutOfScope","Notes"
    ]

    with open("output/deals_week.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    newsletter_input = {"week": week, "items": rows}
    try:
        html = openai_chat(
            [
                {"role": "system", "content": NEWSLETTER_PROMPT},
                {"role": "user", "content": json.dumps(newsletter_input, ensure_ascii=False)},
            ],
            timeout=180,
            max_retries=3,
        )
    except Exception as e:
        print(f"Newsletter generation failed, using fallback. Reason: {e}")
        html = make_fallback_newsletter_html(week, rows)

    final_html = text_to_html(html)

    with open("output/newsletter.html", "w", encoding="utf-8") as f:
        f.write(final_html)

    with open("newsletter.html", "w", encoding="utf-8") as f:
        f.write(final_html)

    print("OK - Files written to output/")

if __name__ == "__main__":
    main()
