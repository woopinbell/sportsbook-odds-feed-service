package com.sportsbook.oddsfeed.api;

import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.protocol.value.EventId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Read-only event listing for clients (typically the gateway and admin-api). All responses use
 * camelCase JSON via Jackson; pagination is cursor-based per ADR-0004 with the cursor encoding
 * {@code "{kickoffInstant}|{eventUuid}"} in URL-safe base64.
 */
@RestController
@RequestMapping("/api/v1/events")
public class EventReadController {

  private static final int DEFAULT_PAGE_SIZE = 20;
  private static final int MAX_PAGE_SIZE = 100;

  private final EventCatalog catalog;

  public EventReadController(EventCatalog catalog) {
    this.catalog = catalog;
  }

  @GetMapping
  public CursorPage<EventSummary> list(
      @RequestParam(value = "cursor", required = false) String cursor,
      @RequestParam(value = "size", defaultValue = "20") int requestedSize) {
    int size = clampSize(requestedSize);
    List<EventSummary> all = catalog.orderedByKickoff();
    int startIndex = cursor == null ? 0 : indexAfter(all, decodeCursor(cursor));
    int endIndex = Math.min(startIndex + size, all.size());
    List<EventSummary> page = all.subList(startIndex, endIndex);
    String nextCursor = endIndex < all.size() ? encodeCursor(page.get(page.size() - 1)) : null;
    return new CursorPage<>(page, nextCursor);
  }

  @GetMapping("/{eventId}")
  public ResponseEntity<EventSummary> get(@PathVariable("eventId") UUID eventId) {
    return catalog
        .get(new EventId(eventId))
        .map(ResponseEntity::ok)
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    org.springframework.http.HttpStatus.NOT_FOUND, "Event not found"));
  }

  static int clampSize(int requestedSize) {
    if (requestedSize <= 0) {
      return DEFAULT_PAGE_SIZE;
    }
    return Math.min(requestedSize, MAX_PAGE_SIZE);
  }

  static String encodeCursor(EventSummary summary) {
    String raw = summary.scheduledStartAt().toString() + "|" + summary.eventId().value();
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  static Cursor decodeCursor(String encoded) {
    try {
      String raw = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
      int sep = raw.indexOf('|');
      if (sep < 0) {
        throw new IllegalArgumentException("missing separator in cursor");
      }
      return new Cursor(
          Instant.parse(raw.substring(0, sep)), UUID.fromString(raw.substring(sep + 1)));
    } catch (IllegalArgumentException | java.time.format.DateTimeParseException e) {
      throw new ResponseStatusException(
          org.springframework.http.HttpStatus.BAD_REQUEST, "Invalid cursor", e);
    }
  }

  private static int indexAfter(List<EventSummary> all, Cursor cursor) {
    for (int i = 0; i < all.size(); i++) {
      EventSummary s = all.get(i);
      int kickoffCompare = s.scheduledStartAt().compareTo(cursor.kickoff());
      if (kickoffCompare > 0) {
        return i;
      }
      if (kickoffCompare == 0 && s.eventId().value().compareTo(cursor.eventId()) > 0) {
        return i;
      }
    }
    return all.size();
  }

  record Cursor(Instant kickoff, UUID eventId) {}
}
