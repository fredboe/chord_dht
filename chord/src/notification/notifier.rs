use futures::Stream;

use std::collections::{HashMap, HashSet};

use std::hash::Hash;

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};

/// # Explanation
/// This struct acts as a channel between the chord layer and the data layer. It is responsible
/// for notifying the data layer of any key transfers and other information.
///
/// The type parameter T is the notification type. The type parameter C (standing for characteristic) is the subscription type.
/// So that this struct is useful T needs to implement Into<Vec<C>> and with that specifying all the characteristic of the notification.
/// An user can now subscribe to a specific characteristic and then be notified of all the notification that have that characteristic.
pub struct Notifier<C, T> {
    finalized: Mutex<bool>,
    notifications: Mutex<HashSet<T>>,
    subscriptions: Mutex<HashMap<C, Vec<Sender<T>>>>,
}

impl<C, T> Notifier<C, T> {
    pub fn new() -> Self {
        Notifier {
            finalized: Mutex::new(false),
            notifications: Mutex::new(HashSet::new()),
            subscriptions: Mutex::new(HashMap::new()),
        }
    }

    /// # Explanation
    /// With the finalize function one essetially locks the whole notification system.
    /// No new notifications can be published and all the subscriptions are deleted.
    pub async fn finalize(&self) {
        let mut finalized = self.finalized.lock().await;
        let _ = std::mem::replace(&mut *finalized, true);
        let mut subscriptions = self.subscriptions.lock().await;
        let _ = std::mem::replace(&mut *subscriptions, HashMap::new());
    }
}

impl<C, T: Eq + Hash> Notifier<C, T> {
    /// # Explanation
    /// Check if a specific notification has been published.
    pub async fn has(&self, x: &T) -> bool {
        let notifications = self.notifications.lock().await;
        notifications.contains(x)
    }

    /// # Explantion
    /// Check if a specific notification has been published and if so then remove it so that this
    /// notification can be published again.
    pub async fn has_and_remove(&self, x: &T) -> bool {
        let mut notifications = self.notifications.lock().await;
        notifications.remove(x)
    }
}

impl<C: Eq + Hash, T: Eq + Hash + Clone + Into<Vec<C>>> Notifier<C, T> {
    /// # Explanation
    /// Publish a new notification.
    pub async fn notify(&self, x: T) {
        if !*self.finalized.lock().await {
            self.notify_subscribers(&x).await;
            self.insert_notification(x).await;
        }
    }

    /// # Explanation
    /// This function sends the notification to all the subscribers that are subscribed to one
    /// of the notification's characteristics.
    async fn notify_subscribers(&self, x: &T) {
        for charactersitic in x.clone().into() {
            let subscriptions = self.subscriptions.lock().await;
            let empty_vec = vec![];

            for subscription_sender in subscriptions.get(&charactersitic).unwrap_or(&empty_vec) {
                subscription_sender.send(x.clone()).await.ok();
            }
        }
    }

    /// # Explanation
    /// Inserts the given notification into the notification set.
    async fn insert_notification(&self, x: T) {
        let mut notifications = self.notifications.lock().await;
        notifications.insert(x);
    }

    /// # Explanation
    /// This function creates a new subscription for the given characteristic.
    pub async fn subscribe(&self, characteristic: C) -> Option<Subscription<T>> {
        if *self.finalized.lock().await {
            return None;
        }

        let mut subscriptions = self.subscriptions.lock().await;
        let (subscription_sender, subscription_receiver) = mpsc::channel(128);

        if let Some(subs_of_c) = subscriptions.get_mut(&characteristic) {
            subs_of_c.push(subscription_sender);
        } else {
            subscriptions.insert(characteristic, vec![subscription_sender]);
        }

        Some(Subscription::new(subscription_receiver))
    }
}

/// # Explanation
/// A subscription is basically just a channel for notifications.
pub struct Subscription<T> {
    receiver: Receiver<T>,
}

impl<T> Subscription<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        Subscription { receiver }
    }
}

impl<T> Stream for Subscription<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
