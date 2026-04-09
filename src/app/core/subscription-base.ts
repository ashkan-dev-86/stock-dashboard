import { Subject } from 'rxjs';

export abstract class SubscriptionBase {
  constructor() {
    this.destroy$.next(true);
  }
  
  public destroy$ = new Subject<boolean>();
  
  protected destroySubs() {
    this.destroy$.next(false);
    this.destroy$.complete();
  }
}