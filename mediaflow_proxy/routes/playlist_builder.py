import json
import logging
import urllib.parse
import os  # Added for environment variable access
from typing import Iterator, Dict, Optional, AsyncIterator
from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.responses import RedirectResponse
import httpx
from mediaflow_proxy.configs import settings
from mediaflow_proxy.utils.http_utils import get_original_scheme
import asyncio

logger = logging.getLogger(__name__)
playlist_builder_router = APIRouter()

# Constants for safety
MAX_PLAYLIST_SIZE = 10 * 1024 * 1024  # 10MB limit per playlist to prevent DoS
TIMEOUT_SECONDS = 30

async def rewrite_m3u_links_streaming(m3u_lines_iterator: Iterator[str], base_url: str, api_password: Optional[str]) -> AsyncIterator[str]:
    """
    Rewrites M3U links from an iterator of lines according to specified rules,
    including headers from #EXTVLCOPT and #EXTHTTP. Yields rewritten lines asynchronously.
    Runs in a thread pool to avoid blocking the event loop.
    """
    # Run the sync generator in a thread to make it async-compatible
    def _sync_rewrite():
        current_ext_headers: Dict[str, str] = {}
        for line_with_newline in m3u_lines_iterator:
            line_content = line_with_newline.rstrip('\n')
            logical_line = line_content.strip()
            is_header_tag = False

            if logical_line.startswith('#EXTVLCOPT:'):
                is_header_tag = True
                try:
                    option_str = logical_line.split(':', 1)[1].strip()
                    if '=' in option_str:
                        key_vlc, value_vlc = option_str.split('=', 1)
                        key_vlc = key_vlc.strip()
                        value_vlc = value_vlc.strip()
                        # Special handling for http-header containing "Key: Value"
                        if key_vlc == 'http-header' and ':' in value_vlc:
                            # Split only on the first colon to handle values with colons
                            parts = value_vlc.split(':', 1)
                            if len(parts) == 2:
                                header_key, header_value = parts
                                current_ext_headers[header_key.strip()] = header_value.strip()
                        elif key_vlc.startswith('http-'):
                            header_key = key_vlc[len('http-'):]
                            current_ext_headers[header_key] = value_vlc
                except Exception as e:
                    logger.error(f"Error parsing #EXTVLCOPT '{logical_line}': {e}")
                    current_ext_headers = {}  # Reset on error

            elif logical_line.startswith('#EXTHTTP:'):
                is_header_tag = True
                try:
                    json_str = logical_line.split(':', 1)[1].strip()
                    parsed_headers = json.loads(json_str)
                    if isinstance(parsed_headers, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in parsed_headers.items()):
                        current_ext_headers = parsed_headers
                    else:
                        logger.warning(f"Invalid JSON structure in #EXTHTTP '{logical_line}'; resetting headers")
                        current_ext_headers = {}
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON in #EXTHTTP '{logical_line}': {e}")
                    current_ext_headers = {}

            if is_header_tag:
                yield line_with_newline
                continue

            if logical_line and not logical_line.startswith('#') and ('http://' in logical_line or 'https://' in logical_line):
                processed_url_parts = []  # Use list for efficient string building
                if 'pluto.tv' in logical_line:
                    processed_url_parts.append(logical_line)
                elif 'vavoo.to' in logical_line:
                    encoded_url = urllib.parse.quote(logical_line, safe='')
                    processed_url_parts.append(f"{base_url}/proxy/hls/manifest.m3u8?d={encoded_url}")
                elif 'vixsrc.to' in logical_line:
                    encoded_url = urllib.parse.quote(logical_line, safe='')
                    processed_url_parts.append(f"{base_url}/extractor/video?host=VixCloud&redirect_stream=true&d={encoded_url}&max_res=true&no_proxy=true")
                elif '.m3u8' in logical_line:
                    encoded_url = urllib.parse.quote(logical_line, safe='')
                    processed_url_parts.append(f"{base_url}/proxy/hls/manifest.m3u8?d={encoded_url}")
                elif '.mpd' in logical_line:
                    parsed_url = urllib.parse.urlparse(logical_line)
                    query_params = urllib.parse.parse_qs(parsed_url.query, keep_blank_values=True)
                    key_ids = query_params.get('key_id', [])
                    keys = query_params.get('key', [])
                    clean_params = {k: v for k, v in query_params.items() if k not in ['key_id', 'key']}
                    clean_query = urllib.parse.urlencode(clean_params, doseq=True) if clean_params else ''
                    clean_url = urllib.parse.urlunparse((
                        parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                        parsed_url.params, clean_query, parsed_url.fragment
                    ))
                    clean_url_for_param = urllib.parse.quote(clean_url, safe='')
                    processed_url_parts.append(f"{base_url}/proxy/mpd/manifest.m3u8?d={clean_url_for_param}")
                    # Append all key_id and key values if present
                    for kid in key_ids:
                        processed_url_parts.append(f"&key_id={urllib.parse.quote(str(kid))}")
                    for k in keys:
                        processed_url_parts.append(f"&key={urllib.parse.quote(str(k))}")
                elif '.php' in logical_line:
                    encoded_url = urllib.parse.quote(logical_line, safe='')
                    processed_url_parts.append(f"{base_url}/proxy/hls/manifest.m3u8?d={encoded_url}")
                else:
                    encoded_url = urllib.parse.quote(logical_line, safe='')
                    processed_url_parts.append(f"{base_url}/proxy/hls/manifest.m3u8?d={encoded_url}")

                # Build the URL string efficiently
                processed_url_content = ''.join(processed_url_parts)

                # Apply headers
                if current_ext_headers:
                    header_params = [f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in current_ext_headers.items()]
                    processed_url_content += ''.join(header_params)
                    current_ext_headers = {}

                # Add api_password
                if api_password:
                    processed_url_content += f"&api_password={urllib.parse.quote(api_password)}"

                yield processed_url_content + '\n'
            else:
                yield line_with_newline

    # Yield asynchronously from the thread
    for line in await asyncio.to_thread(_sync_rewrite):
        yield line

async def async_download_m3u_playlist(url: str) -> list[str]:
    """
    Downloads an M3U playlist asynchronously and returns the lines.
    Includes logging for proxy detection and environment variables.
    """
    # Log proxy detection for debugging
    proxies_detected = httpx.get_proxies()  # Dict of proxies httpx would use (e.g., from env vars)
    logger.info(f"httpx detected proxies: {proxies_detected}")
    logger.info(f"Env vars - HTTPS_PROXY: {os.getenv('HTTPS_PROXY')}, HTTP_PROXY: {os.getenv('HTTP_PROXY')}")

    headers = {
        'User-Agent': settings.user_agent,
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    lines = []
    total_size = 0
    try:
        async with httpx.AsyncClient(verify=True, timeout=TIMEOUT_SECONDS) as client:
            async with client.stream('GET', url, headers=headers) as response:
                response.raise_for_status()
                async for line_bytes in response.aiter_lines():
                    if isinstance(line_bytes, bytes):
                        decoded_line = line_bytes.decode('utf-8', errors='replace')
                    else:
                        decoded_line = str(line_bytes)
                    line_with_newline = decoded_line + '\n' if decoded_line else ''
                    total_size += len(line_with_newline.encode('utf-8'))
                    if total_size > MAX_PLAYLIST_SIZE:
                        raise HTTPException(status_code=413, detail="Playlist too large")
                    lines.append(line_with_newline)
        logger.info(f"Successfully downloaded playlist from {url}, size: {total_size} bytes")
    except httpx.ConnectError as e:
        logger.error(f"Connection error downloading {url}: {e}")
        raise HTTPException(status_code=502, detail=f"Unable to connect to playlist source: {url}") from e
    except httpx.TimeoutException as e:
        logger.error(f"Timeout downloading {url}: {e}")
        raise HTTPException(status_code=504, detail=f"Timeout downloading playlist: {url}") from e
    except Exception as e:
        logger.error(f"Unexpected error downloading {url}: {e}")
        raise
    return lines

async def async_generate_combined_playlist(playlist_definitions: list[str], base_url: str, api_password: Optional[str]) -> AsyncIterator[str]:
    """
    Generates a combined playlist from multiple definitions, downloading in parallel.
    Ensures #EXTM3U is included at least once.
    """
    playlist_urls = []
    for definition in playlist_definitions:
        if '&' in definition:
            parts = definition.split('&', 1)
            playlist_url_str = parts[1] if len(parts) > 1 else parts[0]
        else:
            playlist_url_str = definition
        playlist_urls.append(playlist_url_str)

    results = await asyncio.gather(*[async_download_m3u_playlist(url) for url in playlist_urls], return_exceptions=True)
    extm3u_emitted = False

    for idx, lines in enumerate(results):
        if isinstance(lines, Exception):
            error_line = f"# ERROR processing playlist {playlist_urls[idx]}: {str(lines)}\n"
            yield error_line
            continue

        playlist_lines: list[str] = lines
        current_playlist_had_lines = False
        first_line_of_this_segment = True

        async for line in rewrite_m3u_links_streaming(iter(playlist_lines), base_url, api_password):
            current_playlist_had_lines = True
            is_extm3u_line = line.strip().startswith('#EXTM3U')

            if not extm3u_emitted:
                yield line
                if is_extm3u_line:
                    extm3u_emitted = True
            else:
                if first_line_of_this_segment and is_extm3u_line:
                    continue  # Skip duplicate #EXTM3U
                else:
                    yield line
            first_line_of_this_segment = False

        if current_playlist_had_lines and not extm3u_emitted:
            extm3u_emitted = True

    # If no playlists had lines, emit a minimal header
    if not extm3u_emitted:
        yield "#EXTM3U\n"

@playlist_builder_router.get("/playlist")
async def proxy_handler(
    request: Request,
    d: str = Query(..., description="Query string with playlist definitions", alias="d"),
    api_password: Optional[str] = Query(None, description="API password for MFP"),
):
    """
    Endpoint for proxying M3U playlists with MFP support.
    Query format: playlist1&url1;playlist2&url2
    Example: https://mfp.com:pass123&http://provider.com/playlist.m3u
    """
    try:
        if not d or not d.strip():
            raise HTTPException(status_code=400, detail="Query string missing or empty")

        playlist_definitions = [def_.strip() for def_ in d.split(';') if def_.strip()]
        if not playlist_definitions:
            raise HTTPException(status_code=400, detail="No valid playlist definitions found")

        # Build base_url safely
        original_scheme = get_original_scheme(request)
        base_url = f"{original_scheme}://{request.url.netloc}"

        # Extract base_url from first definition if present
        if playlist_definitions and '&' in playlist_definitions[0]:
            parts = playlist_definitions[0].split('&', 1)
            if ':' in parts[0]:
                parsed = urllib.parse.urlparse(parts[0])
                if parsed.scheme and parsed.netloc:
                    base_url = f"{parsed.scheme}://{parsed.netloc}"

        async def generate_response() -> AsyncIterator[str]:
            async for line in async_generate_combined_playlist(playlist_definitions, base_url, api_password):
                yield line

        return StreamingResponse(
            generate_response(),
            media_type='application/vnd.apple.mpegurl',
            headers={
                'Content-Disposition': 'attachment; filename="playlist.m3u"',
                'Access-Control-Allow-Origin': '*'
            }
        )
    except HTTPException:
        raise  # Re-raise client errors
    except Exception as e:
        logger.error(f"General error in playlist handler: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}") from e

@playlist_builder_router.get("/builder")
async def url_builder():
    """
    Page with an interface to generate the MFP proxy URL.
    """
    return RedirectResponse(url="/playlist_builder.html")
