from typing import Optional


class TerritoryBase:
    """Base DTO entity for territories"""

    async def map_from_territory_dto(
            self,
            other: dict,
            attribute_mapper: Optional[dict] = None,
            exclude: Optional[list] = None
    ):
        """
        DTO mapper
        
        other: another entity as dict (use .__dict__ over vars())
        attribute_mapper: map attribute from other to entity
        exclude: remove attributes from mapper (if attribute is present in entity, it will be unchanged (default is None))
        """

        for key, value in other.items():
            if exclude is not None and key in exclude:
                continue
            if attribute_mapper is not None and key in attribute_mapper:
                setattr(self, attribute_mapper[key], value)
            else:
                if key != "properties" and key in self.__annotations__.keys():
                    setattr(self, key, value)
        if other["properties"] is not None:
            for key, value in other["properties"].items():
                if key in self.__annotations__.keys():
                    setattr(self, key, value)
