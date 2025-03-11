"""Chat service for handling AI interactions."""

import logging
from typing import (
    AsyncGenerator,
    Optional,
)
from uuid import UUID

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionChunk
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.core.config import settings
from app.core.search_service import SearchService, SearchType
from app.models.chat import ChatMessage, ChatRole

logger = logging.getLogger(__name__)


class ChatService:
    """Service for handling chat interactions with AI."""

    DEFAULT_MODEL = "gpt-4o"
    DEFAULT_MODEL_SETTINGS = {
        "temperature": 0.7,
        "max_tokens": 1000,
        "top_p": 1,
        "frequency_penalty": 0,
        "presence_penalty": 0,
    }

    CONTEXT_PROMPT = """You are an AI assistant with access to a knowledge base.
    Use the following relevant context to help answer the user's question.
    Always format your responses in proper markdown, including:
    - Using proper headers (# ## ###)
    - Formatting code blocks with ```language
    - Using tables with | header | header |
    - Using bullet points and numbered lists
    - Using **bold** and *italic* where appropriate

    Here's the context:
    {context}

    Remember to:
    1. Be helpful, clear, and accurate
    2. Maintain a professional tone
    3. Format ALL responses in proper markdown
    4. Use tables when presenting structured data
    5. Use code blocks with proper language tags"""

    def __init__(self):
        """Initialize the chat service with OpenAI client."""
        if not settings.OPENAI_API_KEY:
            logger.warning("OPENAI_API_KEY is not set in environment variables")
            self.client = None
        else:
            self.client = AsyncOpenAI(
                api_key=settings.OPENAI_API_KEY,
            )

    async def generate_streaming_response(
        self,
        db: AsyncSession,
        chat_id: UUID,
        user: schemas.User,
    ) -> AsyncGenerator[ChatCompletionChunk, None]:
        """Generate a streaming AI response.

        Args:
            db (AsyncSession): Database session
            chat_id (UUID): Chat ID
            user (schemas.User): Current user

        Yields:
            AsyncGenerator[ChatCompletionChunk]: Stream of response entities
        """
        try:
            # Check if OpenAI client is initialized
            if not self.client:
                logger.error("OpenAI client not initialized. Check if OPENAI_API_KEY is set.")
                error_message = schemas.ChatMessageCreate(
                    content="Sorry, the AI service is not properly configured. Please contact support.",
                    role=ChatRole.ASSISTANT,
                )
                await crud.chat.add_message(
                    db=db, chat_id=chat_id, obj_in=error_message, current_user=user
                )
                return

            # Get chat and messages
            chat = await crud.chat.get_with_messages(db=db, id=chat_id, current_user=user)
            if not chat:
                logger.error(f"Chat {chat_id} not found")
                return

            # Get relevant context from last user message
            last_user_message = next(
                (msg for msg in reversed(chat.messages) if msg.role == ChatRole.USER), None
            )
            context = ""
            if last_user_message:
                try:
                    context = await self._get_relevant_context(
                        db=db,
                        chat=chat,
                        query=last_user_message.content,
                        user=user,
                    )
                    if context:
                        logger.info(f"Found relevant context ({len(context)} chars) for query")
                    else:
                        logger.info("No relevant context found for query")
                except Exception as context_error:
                    logger.error(f"Error getting context: {str(context_error)}")
                    # Continue without context rather than failing completely

            # Prepare messages with context
            messages = self._prepare_messages_with_context(chat.messages, context)

            # Merge settings
            model = chat.model_name or self.DEFAULT_MODEL

            try:
                # Extract OpenAI API supported parameters
                # Only pass parameters that OpenAI API actually supports
                openai_supported_params = {
                    "temperature": chat.model_settings.get(
                        "temperature", self.DEFAULT_MODEL_SETTINGS["temperature"]
                    ),
                    "max_tokens": chat.model_settings.get(
                        "max_tokens", self.DEFAULT_MODEL_SETTINGS["max_tokens"]
                    ),
                    "top_p": chat.model_settings.get("top_p", self.DEFAULT_MODEL_SETTINGS["top_p"]),
                    "frequency_penalty": chat.model_settings.get(
                        "frequency_penalty", self.DEFAULT_MODEL_SETTINGS["frequency_penalty"]
                    ),
                    "presence_penalty": chat.model_settings.get(
                        "presence_penalty", self.DEFAULT_MODEL_SETTINGS["presence_penalty"]
                    ),
                    "stream": True,  # Enable streaming
                }
            except (AttributeError, TypeError) as e:
                logger.warning(f"Error extracting model settings, using defaults: {e}")
                openai_supported_params = {**self.DEFAULT_MODEL_SETTINGS, "stream": True}

            # Log what we're sending to OpenAI
            logger.info(
                f"Creating completion with model {model} and settings: {openai_supported_params}"
            )

            # Create streaming response
            try:
                stream = await self.client.chat.completions.create(
                    model=model, messages=messages, **openai_supported_params
                )
            except Exception as api_error:
                logger.error(f"OpenAI API error: {str(api_error)}")
                error_message = schemas.ChatMessageCreate(
                    content="Sorry, I encountered an error when calling the AI service. Please try again.",
                    role=ChatRole.ASSISTANT,
                )
                await crud.chat.add_message(
                    db=db, chat_id=chat_id, obj_in=error_message, current_user=user
                )
                raise

            full_content = ""
            async for chunk in stream:
                if chunk.choices[0].delta.content:
                    full_content += chunk.choices[0].delta.content
                yield chunk

            # Save the complete message after streaming
            if full_content:
                message_create = schemas.ChatMessageCreate(
                    content=full_content,
                    role=ChatRole.ASSISTANT,
                )
                await crud.chat.add_message(
                    db=db, chat_id=chat_id, obj_in=message_create, current_user=user
                )

        except Exception as e:
            logger.error(f"Error in stream: {str(e)}")
            # Create error message
            error_message = schemas.ChatMessageCreate(
                content=(
                    "Sorry, I encountered an error while generating a response. Please try again."
                ),
                role=ChatRole.ASSISTANT,
            )
            await crud.chat.add_message(
                db=db, chat_id=chat_id, obj_in=error_message, current_user=user
            )
            raise

    async def generate_and_save_response(
        self,
        db: AsyncSession,
        chat_id: UUID,
        user: schemas.User,
    ) -> Optional[ChatMessage]:
        """Generate a non-streaming AI response and save it."""
        try:
            chat = await crud.chat.get_with_messages(db=db, id=chat_id, current_user=user)
            if not chat:
                logger.error(f"Chat {chat_id} not found")
                return None

            messages = self._prepare_messages(chat.messages)
            model = chat.model_name or self.DEFAULT_MODEL

            # Extract OpenAI API supported parameters
            # Only pass parameters that OpenAI API actually supports
            openai_supported_params = {
                "temperature": chat.model_settings.get(
                    "temperature", self.DEFAULT_MODEL_SETTINGS["temperature"]
                ),
                "max_tokens": chat.model_settings.get(
                    "max_tokens", self.DEFAULT_MODEL_SETTINGS["max_tokens"]
                ),
                "top_p": chat.model_settings.get("top_p", self.DEFAULT_MODEL_SETTINGS["top_p"]),
                "frequency_penalty": chat.model_settings.get(
                    "frequency_penalty", self.DEFAULT_MODEL_SETTINGS["frequency_penalty"]
                ),
                "presence_penalty": chat.model_settings.get(
                    "presence_penalty", self.DEFAULT_MODEL_SETTINGS["presence_penalty"]
                ),
            }

            response = await self.client.chat.completions.create(
                model=model, messages=messages, **openai_supported_params
            )

            if not response.choices:
                logger.error("No response generated from OpenAI")
                return None

            message_create = schemas.ChatMessageCreate(
                content=response.choices[0].message.content,
                role=ChatRole.ASSISTANT,
            )

            return await crud.chat.add_message(
                db=db, chat_id=chat_id, obj_in=message_create, current_user=user
            )

        except Exception as e:
            logger.error(f"Error generating AI response: {str(e)}")
            error_message = schemas.ChatMessageCreate(
                content=(
                    "Sorry, I encountered an error while generating a response. Please try again."
                ),
                role=ChatRole.ASSISTANT,
            )
            return await crud.chat.add_message(
                db=db, chat_id=chat_id, obj_in=error_message, current_user=user
            )

    def _prepare_messages(self, messages: list[ChatMessage]) -> list[dict]:
        """Prepare messages for OpenAI API format."""
        formatted_messages = []
        has_system_message = any(msg.role == ChatRole.SYSTEM for msg in messages)

        if not has_system_message:
            formatted_messages.append(
                {
                    "role": "system",
                    "content": (
                        "You are a helpful AI assistant. Provide clear, accurate, "
                        "and concise responses while being friendly and professional."
                    ),
                }
            )

        formatted_messages.extend(
            [{"role": message.role, "content": message.content} for message in messages]
        )

        return formatted_messages

    async def _get_relevant_context(
        self,
        db: AsyncSession,
        chat: schemas.Chat,
        query: str,
        user: schemas.User,
    ) -> str:
        """Get relevant context for a query.

        Args:
            db (AsyncSession): Database session
            chat (schemas.Chat): Chat object
            query (str): Query to get context for
            user (schemas.User): Current user

        Returns:
            str: Formatted context
        """
        # Initialize the search service
        search_service = SearchService()
        DEFAULT_SEARCH_TYPE = SearchType.VECTOR

        # Basic input validation
        if not query:
            logger.warning("Empty query provided for context retrieval")
            return ""

        try:
            # Get sync ID associated with the chat
            sync_id = chat.sync_id if chat else None
            if not sync_id:
                logger.warning(f"No sync ID associated with chat {chat.id}")
                return ""

            # Determine search type with defensive programming
            search_type = DEFAULT_SEARCH_TYPE

            try:
                # Get available destinations for this sync using the correct method
                sync_destinations = await crud.sync_destination.get_by_sync_id(
                    db=db, sync_id=sync_id
                )

                # Check destination types
                # TODO: generalize this by ensuring destinations have a type indicator like
                # "vector" or "graph"
                vector_dests = [
                    d for d in sync_destinations if d.destination_type == "weaviate_native"
                ]
                graph_dests = [d for d in sync_destinations if d.destination_type == "neo4j_native"]

                # Store available destination types for future reference
                available_search_types = []
                if vector_dests:
                    available_search_types.append(SearchType.VECTOR)
                if graph_dests:
                    available_search_types.append(SearchType.GRAPH)
                if vector_dests and graph_dests:
                    available_search_types.append(SearchType.HYBRID)

                logger.debug(f"Available search types for chat {chat.id}: {available_search_types}")

                # Get requested search type from model_settings
                if chat.model_settings and isinstance(chat.model_settings, dict):
                    search_type_str = chat.model_settings.get("search_type", "").upper()
                    if search_type_str:
                        try:
                            search_type_enum = SearchType[search_type_str]

                            # Validate that the selected search type is available
                            if search_type_enum in available_search_types:
                                search_type = search_type_enum
                                logger.info(f"Using search type from model_settings: {search_type}")
                            else:
                                logger.warning(
                                    f"Search type {search_type_str} selected but not available. "
                                    f"Available: {available_search_types}. Selecting best available option."
                                )
                        except (KeyError, ValueError) as e:
                            logger.warning(f"Invalid search_type in model_settings: {e}")

                # If no valid search type from model_settings, use the best available
                if search_type == DEFAULT_SEARCH_TYPE:
                    if SearchType.HYBRID in available_search_types:
                        search_type = SearchType.HYBRID
                    elif SearchType.GRAPH in available_search_types:
                        search_type = SearchType.GRAPH
                    elif SearchType.VECTOR in available_search_types:
                        search_type = SearchType.VECTOR

                    logger.info(f"Selected best available search type: {search_type}")
            except Exception as e:
                logger.error(f"Error determining available search types: {str(e)}", exc_info=True)
                # Continue with default search type

            logger.info(
                f"Using search type: {search_type} for query: '{query[:50]}...' (truncated)"
            )

            # Search for relevant information
            try:
                results = await search_service.search(
                    db=db,
                    query=query,
                    sync_id=sync_id,
                    current_user=user,
                    search_type=search_type,
                )

                if not results:
                    logger.info(
                        f"No relevant information found for query: '{query[:50]}...' (truncated)"
                    )
                    return ""

                # Format search results as context
                formatted_results = []
                if isinstance(results, dict):
                    # Handle hybrid search results
                    for _search_type_key, results_list in results.items():
                        for result in results_list:
                            formatted_results.append(self._format_search_result(result))
                else:
                    # Handle regular search results
                    for result in results:
                        formatted_results.append(self._format_search_result(result))

                context = "\n\n".join(formatted_results)
                logger.debug(
                    f"Generated context ({len(context)} chars) from {len(formatted_results)} results"
                )
                return context

            except Exception as search_error:
                logger.error(
                    f"Error during {search_type} search: {str(search_error)}", exc_info=True
                )

                # If first search failed and we weren't already using VECTOR, try fallback to VECTOR search
                if search_type != SearchType.VECTOR and SearchType.VECTOR in available_search_types:
                    logger.info(f"Falling back to VECTOR search after {search_type} search failed")
                    try:
                        results = await search_service.search(
                            db=db,
                            query=query,
                            sync_id=sync_id,
                            current_user=user,
                            search_type=SearchType.VECTOR,
                        )

                        if not results:
                            return ""

                        # Format the fallback results
                        formatted_results = [self._format_search_result(r) for r in results]
                        return "\n\n".join(formatted_results)

                    except Exception as fallback_error:
                        logger.error(f"Fallback search error: {str(fallback_error)}", exc_info=True)

                # If we get here, all searches failed
                return ""

        except Exception as e:
            logger.error(f"Error getting context: {str(e)}", exc_info=True)
            return ""

    def _format_search_result(self, result: dict) -> str:
        """Format a search result for inclusion in context.

        Args:
            result: Search result dictionary

        Returns:
            str: Formatted search result
        """
        # Extract common fields
        content = result.get("content", "")
        name = result.get("name", "")
        entity_type = result.get("entity_type", "")

        # Format based on available fields
        if name and content:
            return f"{name} ({entity_type}):\n{content}"
        elif name:
            return f"{name} ({entity_type})"
        elif content:
            return content
        else:
            # Fallback to returning the whole result as a string
            return str(result)

    def _prepare_messages_with_context(
        self,
        messages: list[ChatMessage],
        context: str = "",
    ) -> list[dict]:
        """Prepare messages for OpenAI API format with optional context."""
        formatted_messages = []
        has_system_message = any(msg.role == ChatRole.SYSTEM for msg in messages)

        # Add system message with context if available
        if not has_system_message:
            system_content = (
                self.CONTEXT_PROMPT.format(context=context)
                if context
                else (
                    "You are a helpful AI assistant. "
                    "Always format your responses in proper markdown, "
                    "including tables, code blocks with language tags, and proper headers. "
                    "Provide clear, accurate, and concise responses while being friendly"
                    " and professional."
                )
            )
            formatted_messages.append(
                {
                    "role": "system",
                    "content": system_content,
                }
            )

        # Add chat history
        formatted_messages.extend(
            [{"role": message.role, "content": message.content} for message in messages]
        )

        return formatted_messages

    async def get_context(
        self,
        db: AsyncSession,
        query: str,
        chat_id: UUID,
        current_user: schemas.User,
    ) -> str:
        """Get context for a query in a chat.

        Args:
            db (AsyncSession): Database session
            query (str): Query to get context for
            chat_id (UUID): Chat ID
            current_user (schemas.User): Current user

        Returns:
            str: Formatted context
        """
        try:
            # Get chat
            chat = await crud.chat.get_with_messages(db=db, id=chat_id, current_user=current_user)
            if not chat:
                logger.error(f"Chat {chat_id} not found")
                return ""

            # Get search type
            search_type = SearchType.VECTOR  # Default

            try:
                # Check available destinations
                sync_destinations = await crud.sync_destination.get_by_sync_id(
                    db=db, sync_id=chat.sync_id
                )

                # Check destination types
                # TODO: generalize this by ensuring destinations have a type indicator like
                # "vector" or "graph"
                vector_dests = [
                    d for d in sync_destinations if d.destination_type == "weaviate_native"
                ]
                graph_dests = [d for d in sync_destinations if d.destination_type == "neo4j_native"]

                # Store available destination types
                available_search_types = []
                if vector_dests:
                    available_search_types.append(SearchType.VECTOR)
                if graph_dests:
                    available_search_types.append(SearchType.GRAPH)
                if vector_dests and graph_dests:
                    available_search_types.append(SearchType.HYBRID)

                logger.debug(f"Available search types for chat {chat_id}: {available_search_types}")

                # Get requested search type from model_settings
                if chat.model_settings and isinstance(chat.model_settings, dict):
                    search_type_str = chat.model_settings.get("search_type", "").upper()
                    if search_type_str and search_type_str in SearchType.__members__:
                        requested_type = SearchType[search_type_str]

                        # Make sure requested type is available
                        if requested_type in available_search_types:
                            search_type = requested_type
                            logger.info(f"Using requested search type: {search_type}")
                        else:
                            logger.warning(f"Requested search type {requested_type} not available")

                # If no valid search type from settings, use best available
                if not available_search_types:
                    logger.warning(f"No search destinations found for sync {chat.sync_id}")
                elif search_type not in available_search_types:
                    if SearchType.HYBRID in available_search_types:
                        search_type = SearchType.HYBRID
                    elif SearchType.GRAPH in available_search_types:
                        search_type = SearchType.GRAPH
                    elif SearchType.VECTOR in available_search_types:
                        search_type = SearchType.VECTOR
                    logger.info(f"Selected best available search type: {search_type}")
            except Exception as e:
                logger.error(f"Error determining available search types: {str(e)}", exc_info=True)

            logger.info(f"Using search type: {search_type}")

            # Search for relevant context
            try:
                # Create a search service instance
                search_service_instance = SearchService()
                results = await search_service_instance.search(
                    db=db,
                    query=query,
                    sync_id=chat.sync_id,
                    current_user=current_user,
                    search_type=search_type,
                    limit=10,
                )

                if not results:
                    logger.info(
                        f"No relevant information found for query: '{query[:50]}...' (truncated)"
                    )
                    return ""

                # Format search results
                formatted_results = []
                if isinstance(results, dict):
                    # Handle hybrid search results
                    for _search_type_key, results_list in results.items():
                        for result in results_list:
                            formatted_results.append(self._format_search_result(result))
                else:
                    # Handle regular search results
                    for result in results:
                        formatted_results.append(self._format_search_result(result))

                return "\n\n".join(formatted_results)

            except Exception as e:
                logger.error(f"Error searching for context: {str(e)}", exc_info=True)

                # Try fallback to vector search if that's available and we weren't already using it
                if search_type != SearchType.VECTOR and SearchType.VECTOR in available_search_types:
                    logger.info(f"Falling back to VECTOR search after {search_type} search failed")
                    try:
                        search_service_instance = SearchService()
                        results = await search_service_instance.search(
                            db=db,
                            query=query,
                            sync_id=chat.sync_id,
                            current_user=current_user,
                            search_type=SearchType.VECTOR,
                            limit=10,
                        )

                        if not results:
                            return ""

                        # Format the fallback results
                        formatted_results = [self._format_search_result(r) for r in results]
                        return "\n\n".join(formatted_results)

                    except Exception as fallback_error:
                        logger.error(f"Fallback search error: {str(fallback_error)}", exc_info=True)

                return ""

        except Exception as e:
            logger.error(f"Error getting context: {str(e)}", exc_info=True)
            return ""


# Create a singleton instance
chat_service = ChatService()
