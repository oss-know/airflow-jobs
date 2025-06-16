import torch
from torch.nn import Linear, BatchNorm2d, Module, Flatten, CrossEntropyLoss
from torch import optim


class LinearNetwork(Module):
    def __init__(self):
        super(LinearNetwork, self).__init__()
        self.flatten = Flatten()
        self.linear = Linear(70, 2)

    def forward(self, input):
        input = self.flatten(input).type(torch.float)
        logits = self.linear(input)
        pred = torch.sigmoid(logits)
        return pred


class Trainer():
    def __init__(self) -> None:
        self.model = LinearNetwork()
        self.optimizer = optim.Adam(self.model.parameters(), lr=1e-3)
        self.loss_fn = CrossEntropyLoss()
        # self.loss_l1 = self.l1_regularization()

    def l1_regularization(self, model, l1_alpha):
        l1_loss = []
        for para in model.parameters():
            if type(para) is BatchNorm2d:
                l1_loss.append(torch.abs(para.weight).sum())
        return l1_alpha * sum(l1_loss)


def train(dataloader, trainer: Trainer, device, logger):
    size = len(dataloader.dataset)
    trainer.model.train()
    for batch, (X, y) in enumerate(dataloader):
        X, y = X.to(device), y.to(device)
        pred = trainer.model(X)
        # loss = trainer.loss_fn(pred, y) + trainer.l1_regularization(trainer.model, 0.1)
        loss = trainer.loss_fn(pred, y)

        trainer.optimizer.zero_grad()
        loss.backward()
        trainer.optimizer.step()

        if batch % 5 == 0:
            loss, current = loss.item(), (batch + 1) * len(X)
            print(f"Train: loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")
            logger.info(f"Train: loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def test(dataloader, trainer: Trainer, device, logger):
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    test_loss, correct, incorrect, TP = 0, 0, 0, 0

    trainer.model.eval()
    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
            preds = trainer.model(X)
            test_loss += (trainer.loss_fn(preds, y) + trainer.l1_regularization(trainer.model, 0.1)).item()
            correct += (preds.argmax(1) == y).type(torch.float).sum().item()
            incorrect += (preds.argmax(1) != y).type(torch.float).sum().item()
            TP += (y * preds.argmax(1)).type(torch.float).sum().item()

    test_loss /= num_batches
    acc = correct / size
    f1_score = TP / (TP + 0.5 * incorrect)
    print(f"Test: Accuracy: {(acc):>4f}, F1-Score: {(f1_score):>4f}, Avg loss: {test_loss:>8f} \n")
    logger.info(f"Test: Accuracy: {(acc):>4f}, F1-Score: {(f1_score):>4f}, Avg loss: {test_loss:>8f} \n")
    return acc
