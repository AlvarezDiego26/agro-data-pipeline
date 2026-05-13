import re

from selectolax.parser import HTMLParser, Node


def quick_html_data_signals(html: str | None) -> dict[str, object]:
    """Heuristica barata para distinguir 'HTML vacio' vs 'HTML con tabla/fechas que el parser no leyo'."""
    if not html or not html.strip():
        return {'empty_document': True}
    lowered = html.lower()
    approx_dates = len(re.findall(r'\b\d{1,2}/\d{1,2}/\d{4}\b', html))
    table_open = lowered.count('<table')
    tr_count = lowered.count('<tr')
    return {
        'approx_date_tokens': approx_dates,
        'table_tags': table_open,
        'tr_tags': tr_count,
        'mentions_volumen': 'volumen' in lowered,
        'mentions_precio': 'precio' in lowered,
    }


def extract_tables_with_spans(table: Node) -> list[list[str]]:
    rows = table.css("tr")
    if not rows:
        return []

    matrix = {}  # (row, col) -> text
    max_cols = 0
    
    for r_idx, tr in enumerate(rows):
        c_idx = 0
        cells = tr.css("td, th")
        for cell in cells:
            while (r_idx, c_idx) in matrix:
                c_idx += 1
            
            text = cell.text(strip=True)
            rowspan = int(cell.attributes.get("rowspan", "1") or "1")
            colspan = int(cell.attributes.get("colspan", "1") or "1")
            
            for r in range(rowspan):
                for c in range(colspan):
                    matrix[(r_idx + r, c_idx + c)] = text
            
            c_idx += colspan
            if c_idx > max_cols:
                max_cols = c_idx

    result = []
    actual_max_row = max((r for r, c in matrix.keys()), default=-1)
    for r in range(actual_max_row + 1):
        row_data = []
        for c in range(max_cols):
            row_data.append(matrix.get((r, c), ""))
        if any(row_data):
            result.append(row_data)
    return result


def extract_report_titles(html: str) -> list[str]:
    tree = HTMLParser(html)
    return [node.text(strip=True) for node in tree.css("h1") if node.text(strip=True)]


def extract_dates_from_titles(titles: list[str]) -> list[str]:

    dates = []
    for title in titles:
        # Busca dd/mm/aaaa
        found = re.findall(r'\b\d{1,2}/\d{1,2}/\d{4}\b', title)
        dates.extend(found)
    return dates


def detect_primary_table(html: str) -> list[list[str]]:
    tree = HTMLParser(html)
    table = tree.css_first("table")
    if table is None:
        return []

    rows = extract_tables_with_spans(table)
    if not rows:
        return []

    header = rows[0]

    # Caso 1: Reporte por Intervalo (Pivoteado: Fecha, Producto1, Producto2...)
    if header and "fecha" in header[0].lower():
        if len(rows) >= 4:
             return _extract_mayorista_interval_table_from_rows(rows)

    # Caso 2: Reporte Snapshot (Producto, Variedad, Volumen, Procedencia)
    is_snapshot = any("producto" in h.lower() for h in header) and \
                  any("variedad" in h.lower() for h in header) and \
                  any("volumen" in h.lower() for h in header)

    if is_snapshot:
        titles = extract_report_titles(html)
        dates = extract_dates_from_titles(titles)
        if dates:
            report_date = dates[0]
            new_rows = [["Fecha"] + header]
            for row in rows[1:]:
                new_rows.append([report_date] + row)
            return new_rows

    return rows


def _extract_mayorista_interval_table_from_rows(rows: list[list[str]]) -> list[list[str]]:
    if len(rows) < 4:
        return []

    header_level_1 = rows[0]
    header_level_3 = rows[2]

    if not header_level_1 or not header_level_3:
        return []

    columns = ["Fecha"]
    productos = header_level_1[1:]
    procedencias = header_level_3[1:] if header_level_3[0].lower() == "fecha" else header_level_3

    for idx, producto in enumerate(productos):
        procedencia = procedencias[idx].strip() if idx < len(procedencias) else "Total"
        procedencia = procedencia or "Total"
        columns.append(f"{producto.strip()}__{procedencia}")

    data_rows: list[list[str]] = [columns]
    expected_width = len(columns)
    for row in rows[3:]:
        if row and any(row):
            if len(row) < expected_width:
                row.extend([""] * (expected_width - len(row)))
            elif len(row) > expected_width:
                row = row[:expected_width]
            data_rows.append(row)
    return data_rows


