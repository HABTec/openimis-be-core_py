from promise.dataloader import DataLoader
from promise import Promise

from .models import InteractiveUser


class InteractiveUserLoader(DataLoader):
    def batch_load_fn(self, keys):
        users = {
            user.id: user
            for user in InteractiveUser.objects.filter(id__in=keys)
        }
        return Promise.resolve([users.get(user_id) for user_id in keys])
