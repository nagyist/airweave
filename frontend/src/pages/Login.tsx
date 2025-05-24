import { useEffect } from 'react';
import { Loader2 } from 'lucide-react';
import { useAuth } from '@/lib/auth-context';

const Login = () => {
  const { login, isAuthenticated } = useAuth();

  useEffect(() => {
    // If not authenticated, initiate login flow
    if (!isAuthenticated) {
      login();
    }
  }, [isAuthenticated, login]);

  return (
    <div className="flex h-screen w-full items-center justify-center bg-background">
      <div className="flex flex-col items-center justify-center space-y-4">
        <Loader2 className="h-10 w-10 animate-spin text-primary" />
        <p className="text-muted-foreground">Redirecting to login...</p>
      </div>
    </div>
  );
};

export default Login;
