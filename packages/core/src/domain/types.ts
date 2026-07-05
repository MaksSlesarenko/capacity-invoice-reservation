export type ReservationStatus = 'RESERVED' | 'RELEASED';

export interface Program {
  id: string;
  name: string;
  currency: string;
  totalLimit: string;
  reserved: string;
  version: number;
  updatedAt: Date;
}

export interface Reservation {
  id: string;
  programId: string;
  invoiceId: string;
  invoiceCurrency: string;
  invoiceAmount: string;
  fxRateUsed: string;
  reservedAmount: string;
  status: ReservationStatus;
  createdAt: Date;
  releasedAt: Date | null;
}
