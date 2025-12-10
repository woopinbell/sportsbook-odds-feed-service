package com.sportsbook.oddsfeed;

import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;

class OddsFeedApplicationTests {

  @Test
  void mainMethodIsInvokable() {
    assertThatCode(() -> OddsFeedApplication.class.getDeclaredMethod("main", String[].class))
        .doesNotThrowAnyException();
  }
}
