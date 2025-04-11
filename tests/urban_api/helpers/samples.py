from dataclasses import dataclass

import pytest


@dataclass(frozen=True)
class SimpleInputDTO:
    id: int
    parent_id: int | None
    name: str


@dataclass(frozen=True)
class SimpleOutputDTO:
    id: int
    parent_id: int | None
    name: str
    children: list["SimpleOutputDTO"]


@pytest.fixture
def sample_dtos():
    return [
        SimpleInputDTO(id=1, name="Root", parent_id=None),
        SimpleInputDTO(id=2, name="Child 1", parent_id=1),
        SimpleInputDTO(id=3, name="Child 2", parent_id=1),
        SimpleInputDTO(id=4, name="Grandchild", parent_id=2),
    ]


@pytest.fixture
def expected_hierarchy():
    root = SimpleOutputDTO(id=1, name="Root", parent_id=None, children=[])
    child1 = SimpleOutputDTO(id=2, name="Child 1", parent_id=1, children=[])
    child2 = SimpleOutputDTO(id=3, name="Child 2", parent_id=1, children=[])
    grandchild = SimpleOutputDTO(id=4, name="Grandchild", parent_id=2, children=[])

    child1.children.append(grandchild)
    root.children.extend([child1, child2])

    return {
        "output_model": SimpleOutputDTO,
        "expected_result": [root],
    }
