"""DAG service."""

from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.schemas.dag import DagEdgeCreate, DagNodeCreate, SyncDagDefinitionCreate


class DagService:
    """DAG service."""

    @staticmethod
    async def create_initial_dag(
        db: AsyncSession,
        *,
        sync_id: UUID,
        current_user: schemas.User,
    ) -> schemas.SyncDagDefinitionCreate:
        """Create an initial DAG with source, entities, and destination."""
        ## Get sync
        sync = await crud.sync.get(db, id=sync_id, current_user=current_user)

        ## Get entities from the source
        source_connection = await crud.connection.get(
            db, id=sync.source_connection_id, current_user=current_user
        )
        if not source_connection:
            raise HTTPException(status_code=404, detail="Source connection not found")

        source = await crud.source.get_by_short_name(db, short_name=source_connection.short_name)
        output_entity_definition_ids = source.output_entity_definition_ids

        entity_definitions = await crud.entity_definition.get_multi_by_ids(
            db, ids=output_entity_definition_ids
        )

        nodes: list[DagNodeCreate] = []
        edges: list[DagEdgeCreate] = []

        # Create source node with pre-set ID
        source_node_id = uuid4()
        source_node = DagNodeCreate(
            id=source_node_id,
            type="source",
            name=source.name,
            connection_id=source_connection.id,
        )
        nodes.append(source_node)

        # Create entity nodes with pre-set IDs
        for entity_definition in entity_definitions:
            entity_node_id = uuid4()
            if "Workspace" in entity_definition.name or "Comment" in entity_definition.name:
                continue
            entity_node = DagNodeCreate(
                id=entity_node_id,
                type="entity",
                name=entity_definition.name,
                entity_definition_id=entity_definition.id,
            )
            nodes.append(entity_node)

            # Create edge from source to entity
            edges.append(
                DagEdgeCreate(
                    from_node_id=source_node_id,
                    to_node_id=entity_node_id,
                )
            )

        # Add a fake file entity node
        file_entity_node_id = uuid4()
        file_entity_node = DagNodeCreate(
            id=file_entity_node_id,
            type="entity",
            name="AsanaPDF",
            config={"type": "file"},  # Add a config to identify it as a file type
        )
        nodes.append(file_entity_node)

        # Create edge from source to file entity
        edges.append(
            DagEdgeCreate(
                from_node_id=source_node_id,
                to_node_id=file_entity_node_id,
            )
        )

        # Create destination node with pre-set ID
        destination_node_id = uuid4()
        if sync.destination_connection_id:
            destination_connection = await crud.connection.get(
                db, id=sync.destination_connection_id, current_user=current_user
            )
            if not destination_connection:
                raise HTTPException(status_code=404, detail="Destination connection not found")

            destination = await crud.destination.get_by_short_name(
                db, short_name=destination_connection.short_name
            )
            destination_node = DagNodeCreate(
                id=destination_node_id,
                type="destination",
                name=destination.name,
                connection_id=destination_connection.id,
            )
        else:
            destination = await crud.destination.get_by_short_name(db, short_name="weaviate_native")
            destination_node = DagNodeCreate(
                id=destination_node_id,
                type="destination",
                name="Native Weaviate",
            )
        nodes.append(destination_node)

        # Create edges from entities to destination (including the file entity)
        for node in nodes[1:-1]:  # Skip source and destination nodes
            edges.append(
                DagEdgeCreate(
                    from_node_id=node.id,
                    to_node_id=destination_node_id,
                )
            )

        sync_dag_definition_create = SyncDagDefinitionCreate(
            name=f"DAG for {sync.name}",
            sync_id=sync_id,
            nodes=nodes,
            edges=edges,
        )

        sync_dag_definition = await crud.sync_dag_definition.create_with_nodes_and_edges(
            db, obj_in=sync_dag_definition_create, current_user=current_user
        )

        return schemas.SyncDagDefinition.model_validate(sync_dag_definition, from_attributes=True)


dag_service = DagService()
