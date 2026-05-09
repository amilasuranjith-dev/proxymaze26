package com.proxymaze.proxymaze.dto;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProxyPoolRequest {
    @JsonProperty("proxies")
    private List<String> proxies;

    @JsonProperty("replace")
    private Boolean replace;

    public boolean isReplace() {
        return Boolean.TRUE.equals(replace);
    }
}

