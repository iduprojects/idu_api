from idu_api.urban_api.dto import TerritoryDTO, TerritoryWithoutGeometryDTO


async def territory_dto_without_geometry(dto: TerritoryDTO) -> TerritoryWithoutGeometryDTO:
    params = {}
    for key, val in dto.__dict__:
        if key in TerritoryWithoutGeometryDTO.__dict__.keys():
            params[key] = val
    return TerritoryWithoutGeometryDTO(**params)
