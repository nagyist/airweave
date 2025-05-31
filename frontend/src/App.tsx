import { useEffect } from 'react';
import { ThemeProvider } from '@/lib/theme-provider';
import { Routes, Route } from 'react-router-dom';
import { Toaster } from '@/components/ui/toaster';

import DashboardLayout from '@/components/DashboardLayout';
import Dashboard from '@/pages/Dashboard';
import CollectionDetailView from "@/pages/CollectionDetailView";
import CollectionsView from "@/pages/CollectionsView";
import Chat from '@/pages/Chat';
import WhiteLabel from '@/pages/WhiteLabel';
import CreateWhiteLabel from '@/pages/CreateWhiteLabel';
import WhiteLabelDetail from '@/pages/WhiteLabelDetail';
import WhiteLabelEdit from '@/pages/WhiteLabelEdit';
import { useCollectionsStore } from '@/lib/stores';
import { NotFound } from '@/pages/NotFound';
import { AuthGuard } from '@/components/AuthGuard';
import Login from '@/pages/Login';
import Callback from '@/pages/Callback';
import { protectedPaths, publicPaths } from '@/constants/paths';
import { AuthCallback } from '@/pages/AuthCallback';

function App() {
  // Initialize collections event listeners when the app loads
  useEffect(() => {
    const unsubscribe = useCollectionsStore.getState().subscribeToEvents();
    return unsubscribe;
  }, []);

  return (
    <ThemeProvider defaultTheme="dark" storageKey="airweave-ui-theme">
      <Routes>
        {/* Public routes */}
        <Route path={publicPaths.login} element={<Login />} />
        <Route path={publicPaths.callback} element={<Callback />} />


        {/* Protected routes */}
        <Route element={<AuthGuard><DashboardLayout /></AuthGuard>}>
          <Route path={protectedPaths.dashboard} element={<Dashboard />} />
          <Route path={protectedPaths.collections} element={<CollectionsView />} />
          <Route path={protectedPaths.collectionDetail} element={<CollectionDetailView />} />
          <Route path={protectedPaths.apiKeys} />
          <Route path={protectedPaths.authCallback} element={<AuthCallback />} />
          <Route path={protectedPaths.whiteLabel} element={<WhiteLabel />} />
          <Route path={protectedPaths.whiteLabelTab} element={<WhiteLabelDetail />} />
          <Route path={protectedPaths.whiteLabelCreate} element={<CreateWhiteLabel />} />
          <Route path={protectedPaths.whiteLabelDetail} element={<WhiteLabelDetail />} />
          <Route path={protectedPaths.whiteLabelEdit} element={<WhiteLabelEdit />} />
        </Route>
        <Route path="*" element={<NotFound />} />
      </Routes>
      <Toaster />
    </ThemeProvider>
  );
}

export default App;
