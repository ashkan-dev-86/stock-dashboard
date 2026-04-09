import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { Observable } from 'rxjs';
import { IStock } from '../../models/stock.model';
import { StockService } from '../../services/stock.service';
import { StockCardComponent } from '../stock-card/stock-card.component';

@Component({
    selector: 'app-dashboard',
    standalone: true,
    imports: [
        CommonModule,
        StockCardComponent
    ],
    templateUrl: './dashboard.component.html',
    styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {
    stocks$!: Observable<IStock[]>;

    constructor(
        private stockService: StockService
    ) { }

    ngOnInit(): void {
        this.stocks$ = this.stockService.stocks$;
    }

    onToggle(ticker: string): void {
        this.stockService.toggleStock(ticker);
    }
}