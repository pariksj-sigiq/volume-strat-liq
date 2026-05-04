import { Button } from "./ui/button";

type Toast = {
  id: string;
  title: string;
  body: string;
};

type ToastStackProps = {
  toasts: Toast[];
  onDismiss: (id: string) => void;
};

export function ToastStack({ toasts, onDismiss }: ToastStackProps) {
  return (
    <div className="toast-stack" aria-live="polite">
      {toasts.map((toast) => (
        <Button className="toast" key={toast.id} type="button" variant="outline" onClick={() => onDismiss(toast.id)}>
          <strong>{toast.title}</strong>
          <span>{toast.body}</span>
        </Button>
      ))}
    </div>
  );
}
