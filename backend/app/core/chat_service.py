"""Chat service for handling AI interactions."""

import logging
from typing import AsyncGenerator, Optional
from uuid import UUID

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionChunk
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.core.config import settings
from app.core.search_service import search_service
from app.core.search_type import SearchType
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
        """Get relevant context for the query.

        Args:
            db: Database session
            chat: Chat object
            query: Query string
            user: Current user

        Returns:
            str: Relevant context as a string
        """
        if not chat.sync_id:
            return ""

        try:
            # Get search type preference from model_settings
            search_type = SearchType.VECTOR  # Default to vector search

            # Get the search_type from model_settings
            if chat.model_settings and isinstance(chat.model_settings, dict):
                search_type_value = chat.model_settings["model_settings"].get("search_type")
                if search_type_value:
                    # Log what we've found for debugging
                    logger.info(f"Found search_type in model_settings: {search_type_value}")

                    # Validate and convert to enum
                    try:
                        search_type = SearchType(search_type_value)
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Invalid search_type value '{search_type_value}': {e}. Defaulting to vector search."
                        )

            logger.info(f"Using search type: {search_type}")

            # Perform search based on the search type
            try:
                results = await search_service.search(
                    db=db,
                    query=query,
                    sync_id=chat.sync_id,
                    current_user=user,
                    search_type=search_type,
                    limit=10,
                )
            except Exception as e:
                logger.error(f"Search error with {search_type} search: {str(e)}")
                # If graph or hybrid search fails, try falling back to vector search
                if search_type != SearchType.VECTOR:
                    logger.info(f"Falling back to vector search after {search_type} search failed")
                    try:
                        results = await search_service.search(
                            db=db,
                            query=query,
                            sync_id=chat.sync_id,
                            current_user=user,
                            search_type=SearchType.VECTOR,
                            limit=10,
                        )
                    except Exception as fallback_error:
                        logger.error(f"Fallback search error: {str(fallback_error)}")
                        return ""
                else:
                    return ""

            # Format results based on search type
            if search_type == SearchType.HYBRID:
                # For hybrid search, combine vector and graph results
                vector_results = results.get("vector", [])
                graph_results = results.get("graph", [])

                # Format vector results
                vector_context = ""
                if vector_results:
                    vector_context = "Vector search results:\n\n"
                    for i, result in enumerate(vector_results[:10], 1):  # Limit to top 10
                        vector_context += f"{i}. {self._format_search_result(result)}\n\n"

                # Format graph results
                graph_context = ""
                if graph_results:
                    graph_context = "Graph search results (showing relationships):\n\n"
                    for i, result in enumerate(graph_results[:10], 1):  # Limit to top 10
                        graph_context += f"{i}. {self._format_search_result(result)}\n\n"

                # Combine contexts
                return f"{vector_context}\n{graph_context}".strip()
            else:
                # For single search type (vector or graph)
                if not results:
                    return ""

                context = ""
                for i, result in enumerate(results[:10], 1):  # Limit to top 10
                    context += f"{i}. {self._format_search_result(result)}\n\n"

                return context

        except Exception as e:
            logger.error(f"Error getting context: {str(e)}")
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
        """Get context for a chat message.

        Args:
            db (AsyncSession): Database session
            query (str): User query
            chat_id (UUID): Chat ID
            current_user (schemas.User): Current user

        Returns:
            str: Context for the chat message
        """
        # Get chat info
        chat = await crud.chat.get(db, id=chat_id, current_user=current_user)
        if not chat:
            logger.warning(f"Chat {chat_id} not found")
            return ""

        # Get search type from model settings
        search_type = SearchType.VECTOR  # Default to vector search
        if chat.model_settings and "search_type" in chat.model_settings:
            try:
                search_type = SearchType(chat.model_settings["search_type"])
                logger.info(f"Using search type: {search_type}")
            except ValueError:
                logger.warning(
                    f"Invalid search type: {chat.model_settings['search_type']}. Using vector search."
                )

        # Search for relevant context
        try:
            results = await search_service.search(
                db=db,
                query=query,
                sync_id=chat.sync_id,
                current_user=current_user,
                search_type=search_type,
                limit=10,
            )
        except Exception as e:
            logger.error(f"Error searching for context: {str(e)}")
            return ""

        # Format results based on search type
        if search_type == SearchType.HYBRID:
            # For hybrid search, results are a dict with 'vector' and 'graph' keys
            if not results:
                return ""

            formatted_results = []

            # Format vector results
            if "vector" in results and results["vector"]:
                formatted_results.append("## Vector Search Results")
                for i, result in enumerate(results["vector"][:10], 1):
                    content = result.get("content", "").strip()
                    if not content:
                        continue
                    formatted_results.append(f"### Result {i}")
                    formatted_results.append(content)
                    formatted_results.append("")

            # Format graph results
            if "graph" in results and results["graph"]:
                formatted_results.append("## Graph Search Results")
                for i, result in enumerate(results["graph"][:10], 1):
                    content = result.get("content", "").strip()
                    if not content:
                        continue
                    formatted_results.append(f"### Result {i}")
                    formatted_results.append(content)
                    formatted_results.append("")

            return "\n".join(formatted_results)
        else:
            # For vector or graph search, results are a list
            if not results:
                return ""

            formatted_results = []
            for i, result in enumerate(results[:10], 1):
                content = result.get("content", "").strip()
                if not content:
                    continue
                formatted_results.append(f"### Result {i}")
                formatted_results.append(content)
                formatted_results.append("")

            return "\n".join(formatted_results)


# Create a singleton instance
chat_service = ChatService()
