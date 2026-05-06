from selectolax.parser import HTMLParser


def _build_tree(html: str) -> HTMLParser:
    return HTMLParser(html)


def extract_hidden_inputs(html: str) -> dict[str, str]:
    tree = _build_tree(html)
    hidden: dict[str, str] = {}
    for node in tree.css("input[type='hidden']"):
        name = node.attributes.get("name") or node.attributes.get("id")
        value = node.attributes.get("value", "")
        if name:
            hidden[name] = value
    return hidden


def extract_select_options(html: str, selector: str) -> list[dict[str, str]]:
    tree = _build_tree(html)
    options: list[dict[str, str]] = []
    for node in tree.css(f"{selector} option"):
        value = node.attributes.get("value", "").strip()
        label = node.text(strip=True)
        if value or label:
            options.append({"value": value, "label": label})
    return options


def extract_named_select_options(html: str, name: str) -> list[dict[str, str]]:
    return extract_select_options(html, f"select[name='{name}']")


def extract_market_options(html: str) -> list[dict[str, str]]:
    return extract_named_select_options(html, "mercado")


def extract_variable_options(html: str) -> list[dict[str, str]]:
    return extract_named_select_options(html, "variables[]")


def extract_procedencia_options(html: str) -> list[dict[str, str]]:
    return extract_named_select_options(html, "procedencias[]")


def extract_checkbox_products(html: str) -> list[dict[str, str]]:
    tree = _build_tree(html)
    products: list[dict[str, str]] = []
    for node in tree.css("#productosCheckBox input[type='checkbox'][name='productos[]']"):
        product_id = node.attributes.get("id", "")
        if product_id.endswith("_CHK") and product_id.startswith("CHK_"):
            label_node = tree.css_first(f"label[for='{product_id}']")
            label = label_node.text(strip=True) if label_node else ""
            value = node.attributes.get("value", "").strip()
            if value and value != "NA":
                products.append({"value": value, "label": label})
    return products


def extract_post_id(html: str) -> str | None:
    tree = _build_tree(html)
    nodes = tree.css("input[name='postID']")
    if not nodes:
        return None
    return nodes[-1].attributes.get("value")

