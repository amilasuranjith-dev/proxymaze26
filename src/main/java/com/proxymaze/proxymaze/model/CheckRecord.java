package com.proxymaze.proxymaze.model;

import java.time.Instant;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CheckRecord {
    @JsonProperty("checked_at")
    private Instant checkedAt;

    @JsonProperty("status")
    private String status;
}

