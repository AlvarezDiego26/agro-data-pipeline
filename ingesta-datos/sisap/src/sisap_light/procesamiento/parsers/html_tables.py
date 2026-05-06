from selectolax.parser import HTMLParser, Node


def _expand_row(node: Node) -> list[str]:
    values: list[str] = []
    for cell in node.css("th, td"):
        text = cell.text(separator=" ", strip=True)
        colspan = int(cell.attributes.get("colspan", "1") or "1")
        values.extend([text] * colspan)
    return values


def extract_tables(html: str) -> list[list[str]]:
    tree = HTMLParser(html)
    rows: list[list[str]] = []
    for tr in tree.css("table tr"):
        values = [cell.text(strip=True) for cell in tr.css("th, td")]
        if any(values):
            rows.append(values)
    return rows


def extract_report_titles(html: str) -> list[str]:
    tree = HTMLParser(html)
    return [node.text(strip=True) for node in tree.css("h1") if node.text(strip=True)]


def _extract_mayorista_interval_table(table: Node) -> list[list[str]]:
    rows = table.css("tr")
    if len(rows) < 4:
        return []

    header_level_1 = _expand_row(rows[0])
    header_level_3 = _expand_row(rows[2])

    if not header_level_1 or not header_level_3:
        return []

    columns = ["Fecha"]
    productos = header_level_1[1:]
    procedencias = header_level_3

    for idx, producto in enumerate(productos):
        procedencia = procedencias[idx].strip() if idx < len(procedencias) else "Total"
        procedencia = procedencia or "Total"
        columns.append(f"{producto.strip()}__{procedencia}")

    data_rows: list[list[str]] = [columns]
    expected_width = len(columns)
    for tr in rows[3:]:
        values = [cell.text(strip=True) for cell in tr.css("td, th")]
        if values and any(values):
            if len(values) < expected_width:
                values.extend([""] * (expected_width - len(values)))
            elif len(values) > expected_width:
                values = values[:expected_width]
            data_rows.append(values)
    return data_rows


def detect_primary_table(html: str) -> list[list[str]]:
    tree = HTMLParser(html)
    table = tree.css_first("table")
    if table is None:
        return []

    row_nodes = table.css("tr")
    if row_nodes and row_nodes[0].attributes.get("class") == "encabezado":
        rows = _extract_mayorista_interval_table(table)
        if rows:
            return rows

    return extract_tables(html)

