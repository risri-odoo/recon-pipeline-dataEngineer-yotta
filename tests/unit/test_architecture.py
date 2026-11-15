"""CLAUDE.md rule 3: presentation -> service -> repository; all SQL lives in recon/repository."""

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "recon"
SQL_START = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|BEGIN|COMMIT|ROLLBACK|PRAGMA|WITH)\s")


def modules(folder: str) -> list[Path]:
    return sorted((ROOT / folder).rglob("*.py"))


def imported_names(tree: ast.AST) -> set[str]:
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_presentation_never_imports_repository():
    for path in modules("presentation"):
        names = imported_names(ast.parse(path.read_text(encoding="utf-8")))
        assert not [n for n in names if n.startswith("recon.repository")], path.name


def test_no_sql_strings_outside_repository():
    for folder in ("presentation", "service", "generator"):
        for path in modules(folder):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                          if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef))
                          and n.body and isinstance(n.body[0], ast.Expr)
                          and isinstance(n.body[0].value, ast.Constant)}
            sql = [n.value for n in ast.walk(tree)
                   if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings
                   and SQL_START.match(n.value)]
            assert sql == [], f"{path.relative_to(ROOT)}: {sql}"
