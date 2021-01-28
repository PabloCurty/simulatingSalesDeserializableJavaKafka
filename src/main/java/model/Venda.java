package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Venda{

    private Long operation;
    private Long client;
    private Integer ticketNumber;
    private BigDecimal totalValue;
    private String status;


    public Venda(Long operation, Long client, Integer ticketNumber, BigDecimal totalValue) {
        this.operation = operation;
        this.client = client;
        this.ticketNumber = ticketNumber;
        this.totalValue = totalValue;
    }
}