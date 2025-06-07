"""Web fetcher transformer using Firecrawl."""

import asyncio
import hashlib
import os
import random
from typing import List
from uuid import uuid4

import aiofiles
from firecrawl import AsyncFirecrawlApp

from airweave.core.config import settings
from airweave.core.logging import logger
from airweave.platform.decorators import transformer
from airweave.platform.entities._base import WebEntity
from airweave.platform.entities.web import WebFileEntity
from airweave.platform.sync.async_helpers import run_in_thread_pool

# Improved connection management
_shared_firecrawl_client = None
_client_semaphore = None
_client_lock = asyncio.Lock()
_httpx_client = None
_temp_dir_created = False


async def get_httpx_client():
    """Get or create shared httpx client with proper connection pooling."""
    global _httpx_client

    if _httpx_client is None:
        import httpx

        # Optimized connection limits for high concurrency
        max_connections = getattr(settings, "WEB_FETCHER_MAX_CONNECTIONS", 200)
        keepalive_connections = getattr(settings, "WEB_FETCHER_KEEPALIVE_CONNECTIONS", 100)

        limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=keepalive_connections,
            keepalive_expiry=60.0,  # Longer keepalive for connection reuse
        )

        # Create client with optimized timeout settings
        _httpx_client = httpx.AsyncClient(
            limits=limits,
            timeout=httpx.Timeout(
                connect=5.0,
                read=60.0,  # Increased for slow sites
                write=30.0,
                pool=60.0,
            ),
            # Performance optimizations
            verify=True,
            http2=True,  # Enable HTTP/2 for better performance
            follow_redirects=True,  # Handle redirects automatically
        )

        logger.info(
            f"🌐 HTTPX_CLIENT_INIT Created shared httpx client "
            f"(max_connections={max_connections}, keepalive={keepalive_connections})"
        )

    return _httpx_client


async def cleanup_httpx_client():
    """Clean up the httpx client when done."""
    global _httpx_client
    if _httpx_client:
        await _httpx_client.aclose()
        _httpx_client = None
        logger.info("🌐 HTTPX_CLIENT_CLEANUP Closed httpx client")


async def ensure_temp_dir():
    """Ensure the temp directory exists (create once)."""
    global _temp_dir_created
    if not _temp_dir_created:
        base_temp_dir = "/tmp/airweave"
        await run_in_thread_pool(os.makedirs, base_temp_dir, exist_ok=True)
        _temp_dir_created = True
        logger.info(f"📁 WEB_TEMP_DIR Created temp directory: {base_temp_dir}")
    return "/tmp/airweave"


async def get_firecrawl_client():
    """Get or create the shared Firecrawl client instance with connection pooling."""
    global _shared_firecrawl_client, _client_semaphore

    async with _client_lock:
        if _shared_firecrawl_client is None:
            firecrawl_api_key = getattr(settings, "FIRECRAWL_API_KEY", None)
            if not firecrawl_api_key:
                raise ValueError("FIRECRAWL_API_KEY must be configured to use web fetcher")

            # Configurable semaphore limit - increase for better throughput
            max_concurrent_requests = getattr(settings, "WEB_FETCHER_MAX_CONCURRENT", 30)
            _client_semaphore = asyncio.Semaphore(max_concurrent_requests)

            # Get the shared httpx client
            httpx_client = await get_httpx_client()

            # Initialize client with our custom httpx client if supported
            try:
                # Try to pass our httpx client to Firecrawl
                _shared_firecrawl_client = AsyncFirecrawlApp(
                    api_key=firecrawl_api_key,
                    client=httpx_client,  # This may not be supported
                )
            except Exception:
                # Fallback to default client
                _shared_firecrawl_client = AsyncFirecrawlApp(
                    api_key=firecrawl_api_key,
                )

            logger.info(
                f"🔗 WEB_CLIENT_INIT Initialized Firecrawl client "
                f"(max_concurrent={max_concurrent_requests})"
            )

    return _shared_firecrawl_client, _client_semaphore


async def _retry_with_backoff(func, *args, max_retries=2, entity_context="", **kwargs):
    """Retry a function with exponential backoff and improved error handling."""
    last_exception = None
    context_prefix = f"{entity_context} " if entity_context else ""

    for attempt in range(max_retries + 1):
        try:
            start_time = asyncio.get_event_loop().time()
            result = await func(*args, **kwargs)
            elapsed = asyncio.get_event_loop().time() - start_time

            if attempt > 0:  # Only log success after retry
                logger.info(
                    f"✅ WEB_SUCCESS [{context_prefix}] "
                    f"Completed in {elapsed:.2f}s on attempt {attempt + 1}"
                )

            return result

        except Exception as e:
            elapsed = asyncio.get_event_loop().time() - start_time
            last_exception = e
            error_type = type(e).__name__
            error_msg = str(e)

            # Check for permanent errors
            permanent_errors = [
                "invalid api key",
                "unauthorized",
                "forbidden",
                "not found",
                "bad request",
                "invalid url",
                "404",
            ]

            # Check for rate limiting errors
            rate_limit_errors = ["rate limit", "too many requests", "quota exceeded", "429"]

            is_permanent = any(pe in error_msg.lower() for pe in permanent_errors)
            is_rate_limited = any(rl in error_msg.lower() for rl in rate_limit_errors)

            if is_permanent:
                logger.error(
                    f"🚫 WEB_PERMANENT_ERROR [{context_prefix}] Non-retryable error: "
                    f"{error_type}: {error_msg}"
                )
                raise e

            if attempt < max_retries:
                if is_rate_limited:
                    # Shorter delay for rate limiting since we have higher concurrency
                    base_delay = 2 ** (attempt + 1)  # 2, 4 seconds
                    jitter = random.uniform(0.5, 1.0)
                    delay = base_delay + jitter

                    logger.warning(
                        f"🚦 WEB_RATE_LIMIT [{context_prefix}] "
                        f"Rate limited, retrying in {delay:.2f}s..."
                    )
                else:
                    # Shorter exponential backoff
                    base_delay = 1 * (attempt + 1)  # 1, 2 seconds
                    jitter = random.uniform(0.1, 0.3)
                    delay = base_delay + jitter

                    logger.warning(
                        f"⚠️  WEB_RETRY [{context_prefix}] Attempt {attempt + 1} failed, "
                        f"retrying in {delay:.2f}s..."
                    )

                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"💥 WEB_FINAL_FAILURE [{context_prefix}] "
                    f"All attempts failed: {error_type}: {error_msg}"
                )

    raise last_exception


@transformer(name="Web Fetcher")
async def web_fetcher(web_entity: WebEntity) -> List[WebFileEntity]:
    """Fetch web content using Firecrawl and convert to FileEntity.

    This transformer:
    1. Takes a WebEntity with a URL
    2. Uses Firecrawl to crawl the URL and convert to markdown
    3. Saves the markdown content to a local temporary file
    4. Returns a list containing a single FileEntity with local_path set,
       ready for file chunker

    Args:
        web_entity: The WebEntity containing the URL to fetch

    Returns:
        List[WebFileEntity]: A list containing the single FileEntity with the web
        content as markdown and local_path set
    """
    entity_context = f"Entity({web_entity.entity_id})"

    logger.info(f"🌐 WEB_START [{entity_context}] Starting web fetch for URL: {web_entity.url}")

    # Import storage manager here to avoid circular imports
    from airweave.platform.storage import storage_manager

    # Check if this is a CTTI entity for special handling
    is_ctti = storage_manager._is_ctti_entity(web_entity)

    if is_ctti:
        logger.info(
            f"🏥 WEB_CTTI [{entity_context}] Detected CTTI entity, using global deduplication"
        )

    # Note: is_fully_processed check for CTTI entities is now done in the source
    # before the entity enters the processor pipeline

    # Check if already processed (for non-CTTI entities)
    if await _is_entity_already_processed(web_entity, is_ctti, entity_context):
        return []

    try:
        # Use retry logic with connection limiting
        scrape_result = await _scrape_web_content(web_entity, entity_context)

        # Create and store file entity
        file_entity = await _create_and_store_file_entity(
            web_entity, scrape_result, is_ctti, entity_context
        )

        return [file_entity]

    except Exception as e:
        logger.error(
            f"💥 WEB_ERROR [{entity_context}] Failed to fetch web content: "
            f"{type(e).__name__}: {str(e)}"
        )
        raise e


async def _is_entity_already_processed(
    web_entity: WebEntity, is_ctti: bool, entity_context: str
) -> bool:
    """Check if entity is already processed."""
    from airweave.platform.storage import storage_manager

    if not is_ctti and hasattr(web_entity, "sync_id") and web_entity.sync_id:
        cache_key = f"{web_entity.sync_id}/{web_entity.entity_id}"
        if await storage_manager.is_entity_fully_processed(cache_key):
            logger.info(f"✅ WEB_CACHED [{entity_context}] Web entity already processed (KEPT)")
            # Mark the entity as fully processed so entity_processor marks it as KEPT
            web_entity.is_fully_processed = True
            return True
    return False


async def _scrape_web_content(web_entity: WebEntity, entity_context: str):
    """Scrape web content using Firecrawl."""

    async def _scrape_with_firecrawl():
        """Internal function to handle the actual scraping with connection limiting."""
        app, semaphore = await get_firecrawl_client()

        # Use semaphore to limit concurrent connections
        async with semaphore:
            logger.info(f"🔗 WEB_CONNECT [{entity_context}] Acquiring connection slot")

            logger.info(f"📥 WEB_SCRAPE [{entity_context}] Scraping URL: {web_entity.url}")
            scrape_start = asyncio.get_event_loop().time()

            # Start with shorter timeout, retry with longer if needed
            timeouts = [10.0, 20.0, 30.0]
            for timeout in timeouts:
                try:
                    scrape_result = await asyncio.wait_for(
                        app.scrape_url(
                            web_entity.url,
                            formats=["markdown"],
                            include_tags=["ctg-study-details-top-info", "ctg-study-info"],
                            only_main_content=True,
                        ),
                        timeout=timeout,
                    )
                    break
                except asyncio.TimeoutError:
                    if timeout == timeouts[-1]:
                        raise

            scrape_elapsed = asyncio.get_event_loop().time() - scrape_start

            if (
                not scrape_result
                or not hasattr(scrape_result, "markdown")
                or not scrape_result.markdown
            ):
                logger.warning(f"📭 WEB_EMPTY [{entity_context}] No markdown content returned")
                raise ValueError(f"No content extracted from URL: {web_entity.url}")

            content_length = len(scrape_result.markdown)
            logger.info(
                f"📄 WEB_CONTENT [{entity_context}] Received {content_length} characters "
                f"in {scrape_elapsed:.2f}s"
            )

            return scrape_result

    return await _retry_with_backoff(_scrape_with_firecrawl, entity_context=entity_context)


async def _create_and_store_file_entity(
    web_entity: WebEntity, scrape_result, is_ctti: bool, entity_context: str
) -> WebFileEntity:
    """Create file entity from scraped content and store it."""
    # Streamlined file processing
    logger.info(f"💾 WEB_FILE_START [{entity_context}] Creating temporary file")

    # Get markdown content and metadata
    markdown_content = scrape_result.markdown
    metadata = scrape_result.metadata if hasattr(scrape_result, "metadata") else {}

    # Extract title
    if isinstance(metadata, dict):
        title = web_entity.title or metadata.get("title", "Web Page")
    else:
        title = web_entity.title or getattr(metadata, "title", "Web Page")

    # Generate file metadata
    file_uuid = uuid4()
    safe_title = title.replace("/", "_").replace("\\", "_")[:100]  # Limit title length
    safe_filename = f"{file_uuid}-{safe_title}.md"

    # Use storage manager's temp directory
    base_temp_dir = "/tmp/airweave/processing"
    os.makedirs(base_temp_dir, exist_ok=True)
    temp_file_path = os.path.join(base_temp_dir, safe_filename)

    # Write file asynchronously
    async with aiofiles.open(temp_file_path, "w", encoding="utf-8") as f:
        await f.write(markdown_content)

    # Calculate file size and checksum in parallel with file write
    def _calculate_file_metrics(content: str):
        encoded_content = content.encode("utf-8")
        file_size = len(encoded_content)
        checksum = hashlib.sha256(encoded_content).hexdigest()
        return file_size, checksum

    file_size, checksum = await run_in_thread_pool(_calculate_file_metrics, markdown_content)

    # Create enhanced metadata
    enhanced_metadata = _create_enhanced_metadata(web_entity, title, markdown_content, is_ctti)

    # Create WebFileEntity
    file_entity = _create_web_file_entity(
        web_entity,
        title,
        file_size,
        temp_file_path,
        file_uuid,
        checksum,
        metadata,
        enhanced_metadata,
    )

    # Store in persistent storage
    await _store_file_entity(file_entity, temp_file_path, is_ctti, entity_context)

    logger.info(
        f"✅ WEB_COMPLETE [{entity_context}] Successfully created FileEntity ({file_size} bytes)"
    )

    return file_entity


def _create_enhanced_metadata(
    web_entity: WebEntity, title: str, markdown_content: str, is_ctti: bool
) -> dict:
    """Create enhanced metadata for the file entity."""
    enhanced_metadata = {
        "url": web_entity.url,
        "title": title,
        "content_length": len(markdown_content),
        **(web_entity.metadata or {}),
    }

    # Add CTTI source marker if this is a CTTI entity
    if is_ctti:
        enhanced_metadata["source"] = "CTTI"

    return enhanced_metadata


def _create_web_file_entity(
    web_entity: WebEntity,
    title: str,
    file_size: int,
    temp_file_path: str,
    file_uuid,
    checksum: str,
    metadata,
    enhanced_metadata: dict,
) -> WebFileEntity:
    """Create WebFileEntity with optimized field copying."""
    return WebFileEntity(
        entity_id=web_entity.entity_id,
        file_id=f"web_{web_entity.entity_id}",
        name=f"{title}.md",
        mime_type="text/markdown",
        size=file_size,
        download_url=web_entity.url,
        url=web_entity.url,
        local_path=temp_file_path,
        file_uuid=file_uuid,
        checksum=checksum,
        total_size=file_size,
        # Copy BaseEntity fields
        breadcrumbs=web_entity.breadcrumbs,
        parent_entity_id=web_entity.parent_entity_id,
        sync_id=web_entity.sync_id,
        sync_job_id=web_entity.sync_job_id,
        source_name=web_entity.source_name,
        sync_metadata=web_entity.sync_metadata,
        # WebFileEntity specific
        original_url=web_entity.url,
        crawl_metadata=metadata,
        web_title=title,
        web_description=metadata.get("description", "") if isinstance(metadata, dict) else "",
        # Enhanced metadata
        metadata=enhanced_metadata,
    )


async def _store_file_entity(
    file_entity: WebFileEntity, temp_file_path: str, is_ctti: bool, entity_context: str
) -> None:
    """Store file entity in persistent storage."""
    from airweave.platform.storage import storage_manager

    if is_ctti:
        # Use CTTI-specific storage (global deduplication)
        with open(temp_file_path, "rb") as f:
            file_entity = await storage_manager.store_ctti_file(file_entity, f)

        logger.info(
            f"💾 WEB_CTTI_STORED [{entity_context}] "
            f"Stored CTTI file globally: {file_entity.storage_blob_name}"
        )
    else:
        # Non-CTTI: Use standard sync-based storage
        # (file will be uploaded by file_manager when needed)
        logger.info(
            f"💾 WEB_FILE_CREATED [{entity_context}] Created local file at {temp_file_path}"
        )
